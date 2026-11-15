// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashSet, sync::Arc};

use dag::{
    block::{Block, BlockReference, RoundNumber},
    committee::{Committee, Stake, StakeAggregator},
    data::Data,
    storage::BlockReader,
};

use crate::{leader::LeaderElector, protocol::Protocol};

/// Adaptive controller for the number of leaders per round.
///
/// Every `interval` rounds it measures the direct-commit rate over the most
/// recently committed sub-dag and adjusts the leader count: additive increase
/// when the network is healthy, multiplicative (exponential) backoff when it
/// is not.
pub(crate) struct LeaderScheduler {
    committee: Arc<Committee>,
    leader_elector: LeaderElector,
    /// Wave length of the active protocol.
    wave_length: RoundNumber,
    /// Certificates required for a direct commit (the outer threshold).
    direct_commit_quorum: Stake,
    /// Votes a decision-round block must reference to be a certificate (the
    /// inner threshold); coincides with `direct_commit_quorum` for single-tier
    /// protocols but not for dual-path ones such as DagHydrangea.
    certificate_quorum: Stake,
    /// Whether the active protocol pipelines (a leader every round).
    pipeline: bool,
    /// Upper bound on the leader count.
    max_leaders_per_round: usize,
    /// Rounds between reconfigurations.
    interval: RoundNumber,
    /// Round of the last reconfiguration.
    last_round: RoundNumber,
    /// Current number of leaders per round.
    number_of_leaders: usize,
    /// Consecutive backoff steps; drives the exponential decrease.
    consecutive_decreases: u32,
}

impl LeaderScheduler {
    pub(crate) fn new(protocol: &Protocol, committee: Arc<Committee>) -> Self {
        let leader_elector = LeaderElector::new(committee.len());
        Self {
            committee,
            leader_elector,
            wave_length: protocol.wave_length,
            direct_commit_quorum: protocol.direct_commit_quorum,
            certificate_quorum: protocol.certificate_quorum,
            pipeline: protocol.pipeline,
            max_leaders_per_round: protocol.max_leaders_per_round.get(),
            interval: protocol.scheduler_interval,
            last_round: 0,
            number_of_leaders: protocol.leader_count.get(),
            consecutive_decreases: 0,
        }
    }

    /// Whether `round` is far enough past the last reconfiguration to trigger
    /// a new one: strictly more than `interval` rounds, so that the next
    /// measurement window (`interval + 1` rounds ending at the new anchor, see
    /// `collect_subdag`) starts after the previous anchor's round.
    pub(crate) fn is_schedule_round(&self, round: RoundNumber) -> bool {
        if round == 0 || round <= self.last_round {
            return false;
        }
        (round - self.last_round) > self.interval
    }

    /// Measure the commit rate over the latest sub-dag and return the new
    /// leader count. `sequence_leader` is the committed leader at the schedule
    /// round.
    pub(crate) fn update_leader(
        &mut self,
        block_reader: &BlockReader,
        sequence_leader: Data<Block>,
    ) -> usize {
        let anchor_round = sequence_leader.round();
        let subdag = self.collect_subdag(block_reader, sequence_leader);

        let expected_commits = self.calculate_expected_commits(self.number_of_leaders);
        if expected_commits == 0 {
            self.last_round = anchor_round;
            return self.number_of_leaders;
        }

        let count = self.count_direct_commits(&subdag, block_reader, self.number_of_leaders);
        let rate = count as f64 / expected_commits as f64;

        let new_value = if rate >= 0.96 {
            // Additive increase.
            self.consecutive_decreases = 0;
            (self.number_of_leaders + 1).min(self.max_leaders_per_round)
        } else {
            // Exponential backoff.
            let decrement = 1usize
                .checked_shl(self.consecutive_decreases)
                .unwrap_or(usize::MAX);
            self.consecutive_decreases = self.consecutive_decreases.saturating_add(1);
            self.number_of_leaders.saturating_sub(decrement).max(1)
        };

        tracing::info!(
            "Leader scheduler: round {anchor_round}, commit rate {rate:.3} \
             ({count}/{expected_commits}), leaders {} -> {new_value}",
            self.number_of_leaders,
        );

        self.last_round = anchor_round;
        self.number_of_leaders = new_value;
        new_value
    }

    /// Collect the sub-dag rooted at `anchor`: the anchor's causal history over
    /// the `interval + 1` rounds `anchor - interval ..= anchor`.
    ///
    /// The top round holds only the anchor itself (the walk descends through
    /// `includes`), so a leader whose decision round is the anchor round can
    /// never gather a certificate quorum inside the sub-dag. The leader rounds
    /// decidable inside it are therefore `anchor - interval ..= anchor - wave_length`,
    /// `interval - (wave_length - 1)` of them — exactly what
    /// `calculate_expected_commits` assumes, so the measured rate never exceeds 1.
    fn collect_subdag(&self, block_reader: &BlockReader, anchor: Data<Block>) -> Vec<Data<Block>> {
        // `saturating_sub` guards against underflow when the anchor round is
        // below the interval.
        let until = anchor.round().saturating_sub(self.interval);
        let mut visited = HashSet::new();
        let mut to_visit = vec![anchor];
        let mut subdag = Vec::new();

        while let Some(block) = to_visit.pop() {
            if visited.insert(*block.reference()) {
                for include_ref in block.includes() {
                    if include_ref.round() >= until
                        && !visited.contains(include_ref)
                        && let Some(included) = block_reader.get_block(*include_ref)
                    {
                        to_visit.push(included);
                    }
                }
                subdag.push(block);
            }
        }
        subdag
    }

    /// Expected number of direct commits in the analysis window: the leader
    /// rounds whose decision round lies strictly inside the sub-dag collected by
    /// `collect_subdag`.
    fn calculate_expected_commits(&self, leaders_per_round: usize) -> u64 {
        // The top `wave_length - 1` rounds of the window cannot have produced a
        // decision inside it (the top round holds only the anchor).
        let round_count = self.interval.saturating_sub(self.wave_length - 1);
        let wave_length = if self.pipeline { 1 } else { self.wave_length };
        (round_count * leaders_per_round as u64) / wave_length
    }

    /// Count leaders directly committed within `blocks`.
    fn count_direct_commits(
        &self,
        blocks: &[Data<Block>],
        block_reader: &BlockReader,
        leaders_per_round: usize,
    ) -> usize {
        let mut count = 0;

        let mut rounds: Vec<RoundNumber> = blocks.iter().map(|b| b.round()).collect();
        rounds.sort_unstable();
        rounds.dedup();

        for round in rounds {
            // Genesis is never a leader slot (the committer filters it too).
            if round == 0 {
                continue;
            }
            let wave = round / self.wave_length;
            // Without pipelining, only the first round of a wave has leaders.
            if !self.pipeline && wave * self.wave_length != round {
                continue;
            }

            for leader_offset in 0..leaders_per_round as u64 {
                let leader = self.leader_elector.elect_leader(round + leader_offset);
                for leader_block in block_reader.get_blocks_at_authority_round(leader, round) {
                    if self.is_directly_committed(&leader_block, blocks) {
                        count += 1;
                    }
                }
            }
        }

        count
    }

    /// Whether `leader` was directly committed within `blocks`, applying the
    /// active protocol's wave length and quorums.
    ///
    /// Support is propagated transitively: starting from the leader, each round
    /// up to `decision_round - 1` extends the supporter set to blocks that include
    /// any block from the previous supporter set. At `decision_round` (the last
    /// round of the wave) a block is a certificate if it includes
    /// `certificate_quorum` stake of supporters from the preceding round, and
    /// `direct_commit_quorum` stake of certificates is a direct commit — the
    /// same two thresholds `BaseCommitter` applies.
    ///
    /// Wave length 2 is special-cased: the decision round sits one round above
    /// the leader, so votes and certificates coincide (mirroring
    /// `BaseCommitter::is_certificate`) — a decision-round block that includes
    /// the leader is itself a certificate. Works for any wave length ≥ 2.
    fn is_directly_committed(&self, leader: &Data<Block>, blocks: &[Data<Block>]) -> bool {
        let leader_round = leader.round();
        let decision_round = leader_round + self.wave_length - 1;

        // Wave length 2: votes and certificates coincide on the decision round
        // — an `R+1` block that includes the leader is itself a certificate, so
        // the transitive support loop below (empty for wave 2) does not apply.
        if self.wave_length == 2 {
            let mut certificate_stake = StakeAggregator::new(self.direct_commit_quorum);
            for block in blocks.iter().filter(|b| b.round() == decision_round) {
                if block.includes().contains(leader.reference())
                    && certificate_stake.add(block.reference().authority, &self.committee)
                {
                    return true;
                }
            }
            return false;
        }

        // Seed with the leader itself, then propagate support round by round.
        // A block at round r supports the leader if it includes any supporter
        // from round r-1.
        let mut supporters: HashSet<BlockReference> = [*leader.reference()].into();
        for round in (leader_round + 1)..decision_round {
            supporters = blocks
                .iter()
                .filter(|b| b.round() == round)
                .filter(|b| b.includes().iter().any(|r| supporters.contains(r)))
                .map(|b| *b.reference())
                .collect();
            if supporters.is_empty() {
                return false;
            }
        }

        // A decision-round block is a certificate when it includes supporters
        // from a quorum of stake; a quorum of certificates is a direct commit.
        let mut certificate_stake = StakeAggregator::new(self.direct_commit_quorum);
        for block in blocks.iter().filter(|b| b.round() == decision_round) {
            let mut vote_stake = StakeAggregator::new(self.certificate_quorum);
            let has_quorum = supporters
                .iter()
                .filter(|s| block.includes().contains(s))
                .any(|s| vote_stake.add(s.authority, &self.committee));
            if has_quorum && certificate_stake.add(block.reference().authority, &self.committee) {
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use dag::{
        authority::Authority,
        block::{Block, RoundNumber},
        committee::Committee,
        data::Data,
        storage::{BlockReader, Storage},
        test_util::{build_dag, build_dag_layer, committee},
    };

    use super::LeaderScheduler;
    use crate::{leader::LeaderElector, protocol::Protocol};

    // --- helpers -----------------------------------------------------------

    /// A Mysticeti protocol (wave 3, pipelined) with the scheduler knobs set.
    fn mysticeti(
        committee: &Committee,
        leaders: usize,
        max: usize,
        interval: RoundNumber,
    ) -> Protocol {
        let mut protocol =
            Protocol::mysticeti(committee.total_stake(), NonZeroUsize::new(leaders).unwrap());
        protocol.max_leaders_per_round = NonZeroUsize::new(max).unwrap();
        protocol.scheduler_interval = interval;
        protocol
    }

    /// A Blue Bottle protocol (wave 2, pipelined) with the scheduler interval set.
    fn blue_bottle(committee: &Committee, leaders: usize, interval: RoundNumber) -> Protocol {
        let mut protocol =
            Protocol::blue_bottle(committee.total_stake(), NonZeroUsize::new(leaders).unwrap());
        protocol.scheduler_interval = interval;
        protocol
    }

    /// A Cordial Miners protocol (wave 3, *not* pipelined) with the interval set.
    fn cordial_ps(committee: &Committee, interval: RoundNumber) -> Protocol {
        let mut protocol = Protocol::cordial_miners_partially_synchronous(committee.total_stake());
        protocol.scheduler_interval = interval;
        protocol
    }

    fn new_storage(committee: &Committee) -> Storage {
        Storage::new_for_test(committee)
    }

    /// The committed leader block for `round` under round-robin election.
    fn leader_block(
        reader: &BlockReader,
        elector: &LeaderElector,
        round: RoundNumber,
    ) -> Data<Block> {
        reader
            .get_blocks_at_authority_round(elector.elect_leader(round), round)
            .into_iter()
            .next()
            .expect("leader block must exist at the requested round")
    }

    /// Every block in rounds `lo..=hi`, the slice the decision helpers consume.
    fn blocks_in_rounds(
        reader: &BlockReader,
        lo: RoundNumber,
        hi: RoundNumber,
    ) -> Vec<Data<Block>> {
        (lo..=hi)
            .flat_map(|round| reader.get_blocks_by_round(round))
            .collect()
    }

    /// Build a DAG up to `stop` in which every block links *only* to authority
    /// 0's block from the previous round.
    fn build_degraded_dag(committee: &Committee, storage: &mut Storage, stop: RoundNumber) {
        // `build_dag(.., None, 0)` seeds and returns the genesis references.
        let mut refs = build_dag(committee, storage, None, 0);
        let hub = Authority::new(0);
        for _ in 1..=stop {
            let parent = *refs
                .iter()
                .find(|reference| reference.authority == hub)
                .expect("authority 0 produces a block every round");
            let connections = committee
                .authorities()
                .map(|authority| (authority, vec![parent]))
                .collect();
            refs = build_dag_layer(connections, storage);
        }
    }

    // --- is_schedule_round -------------------------------------------------

    /// A fresh scheduler (`last_round == 0`) reconfigures only once a round
    /// strictly more than `interval` past round 0 is reached.
    #[test]
    fn is_schedule_round_initial_window() {
        let committee = committee(4);
        let scheduler = LeaderScheduler::new(&mysticeti(&committee, 1, 5, 10), committee.clone());

        assert!(
            !scheduler.is_schedule_round(0),
            "round 0 is never a schedule round"
        );
        assert!(!scheduler.is_schedule_round(1));
        assert!(
            !scheduler.is_schedule_round(10),
            "exactly `interval` away is not yet due"
        );
        assert!(
            scheduler.is_schedule_round(11),
            "one past `interval` is due"
        );
        assert!(scheduler.is_schedule_round(100));
    }

    /// After a reconfiguration the window slides: rounds at or before
    /// `last_round` never trigger, and the next trigger is `interval + 1` past it.
    #[test]
    fn is_schedule_round_slides_with_last_round() {
        let committee = committee(4);
        let mut scheduler =
            LeaderScheduler::new(&mysticeti(&committee, 1, 5, 10), committee.clone());
        scheduler.last_round = 50;

        assert!(!scheduler.is_schedule_round(40), "before last_round");
        assert!(!scheduler.is_schedule_round(50), "exactly at last_round");
        assert!(
            !scheduler.is_schedule_round(60),
            "exactly `interval` past last_round"
        );
        assert!(scheduler.is_schedule_round(61), "one past the window");
    }

    // --- calculate_expected_commits ---------------------------------------

    /// Pipelined wave-3: every round has leaders, and the two trailing rounds
    /// of the window cannot have decided yet -> `(interval - 2) * leaders`.
    #[test]
    fn expected_commits_pipelined() {
        let committee = committee(4);
        let scheduler = LeaderScheduler::new(&mysticeti(&committee, 1, 5, 50), committee.clone());
        assert_eq!(scheduler.calculate_expected_commits(1), 48);
        assert_eq!(scheduler.calculate_expected_commits(3), 144);

        // Windows too short to decide anything yield zero.
        let tiny = LeaderScheduler::new(&mysticeti(&committee, 1, 5, 2), committee.clone());
        assert_eq!(tiny.calculate_expected_commits(5), 0);
        let tinier = LeaderScheduler::new(&mysticeti(&committee, 1, 5, 1), committee.clone());
        assert_eq!(tinier.calculate_expected_commits(5), 0);
    }

    /// Non-pipelined wave-3: only one round per wave carries leaders, so the
    /// count is divided by the wave length.
    #[test]
    fn expected_commits_non_pipelined() {
        let committee = committee(4);
        let scheduler = LeaderScheduler::new(&cordial_ps(&committee, 50), committee.clone());
        // round_count = 50 - 2 = 48; divided by wave length 3.
        assert_eq!(scheduler.calculate_expected_commits(1), 16);
        assert_eq!(scheduler.calculate_expected_commits(3), 48);
    }

    /// Wave-2 (pipelined): only the single trailing round cannot have decided
    /// yet -> `(interval - 1) * leaders`.
    #[test]
    fn expected_commits_wave_two() {
        let committee = committee(4);
        let scheduler = LeaderScheduler::new(&blue_bottle(&committee, 1, 50), committee.clone());
        assert_eq!(scheduler.calculate_expected_commits(1), 49);
        assert_eq!(scheduler.calculate_expected_commits(2), 98);
    }

    // --- collect_subdag ----------------------------------------------------

    /// The sub-dag spans exactly `[anchor - interval, anchor]`; only the anchor
    /// occupies the top round (the walk descends through `includes`).
    #[test]
    fn collect_subdag_spans_one_interval() {
        let committee = committee(4);
        let mut storage = new_storage(&committee);
        build_dag(&committee, &mut storage, None, 12);
        let reader = storage.block_reader().clone();
        let elector = LeaderElector::new(committee.len());
        let scheduler = LeaderScheduler::new(&mysticeti(&committee, 1, 5, 10), committee.clone());

        let anchor = leader_block(&reader, &elector, 12);
        let subdag = scheduler.collect_subdag(&reader, anchor);

        let mut rounds: Vec<RoundNumber> = subdag.iter().map(|block| block.round()).collect();
        rounds.sort_unstable();
        rounds.dedup();
        assert_eq!(rounds, (2..=12).collect::<Vec<_>>());
        assert_eq!(
            subdag.iter().filter(|block| block.round() == 12).count(),
            1,
            "only the anchor sits on the top round",
        );
        assert_eq!(subdag.iter().filter(|block| block.round() == 11).count(), 4);
    }

    /// `saturating_sub` keeps the walk from underflowing when the anchor round
    /// is below the interval: the window simply bottoms out at genesis.
    #[test]
    fn collect_subdag_saturates_at_genesis() {
        let committee = committee(4);
        let mut storage = new_storage(&committee);
        build_dag(&committee, &mut storage, None, 5);
        let reader = storage.block_reader().clone();
        let elector = LeaderElector::new(committee.len());
        let scheduler = LeaderScheduler::new(&mysticeti(&committee, 1, 5, 10), committee.clone());

        let anchor = leader_block(&reader, &elector, 5);
        let subdag = scheduler.collect_subdag(&reader, anchor);

        let min = subdag.iter().map(|block| block.round()).min().unwrap();
        assert_eq!(
            min, 0,
            "until = 5.saturating_sub(10) = 0, so genesis is included"
        );
    }

    // --- is_directly_committed --------------------------------------------

    /// Wave 3, full mesh: the leader is supported and certified by a quorum.
    #[test]
    fn directly_committed_wave3_full_mesh() {
        let committee = committee(4);
        let mut storage = new_storage(&committee);
        build_dag(&committee, &mut storage, None, 5);
        let reader = storage.block_reader().clone();
        let elector = LeaderElector::new(committee.len());
        let scheduler = LeaderScheduler::new(&mysticeti(&committee, 1, 5, 10), committee.clone());

        let leader = leader_block(&reader, &elector, 3);
        let blocks = blocks_in_rounds(&reader, 3, 5);
        assert!(scheduler.is_directly_committed(&leader, &blocks));
    }

    /// Wave 3: without the decision round (`leader + 2`) in the slice there can
    /// be no certificate, so the leader is undecided here.
    #[test]
    fn not_directly_committed_wave3_without_decision_round() {
        let committee = committee(4);
        let mut storage = new_storage(&committee);
        build_dag(&committee, &mut storage, None, 5);
        let reader = storage.block_reader().clone();
        let elector = LeaderElector::new(committee.len());
        let scheduler = LeaderScheduler::new(&mysticeti(&committee, 1, 5, 10), committee.clone());

        let leader = leader_block(&reader, &elector, 3);
        let blocks = blocks_in_rounds(&reader, 3, 4); // decision round 5 omitted
        assert!(!scheduler.is_directly_committed(&leader, &blocks));
    }

    /// Wave 3, degraded: no block at `leader + 1` votes for the leader, so
    /// support never propagates.
    #[test]
    fn not_directly_committed_wave3_unsupported() {
        let committee = committee(4);
        let mut storage = new_storage(&committee);
        build_degraded_dag(&committee, &mut storage, 5);
        let reader = storage.block_reader().clone();
        let elector = LeaderElector::new(committee.len());
        let scheduler = LeaderScheduler::new(&mysticeti(&committee, 1, 5, 10), committee.clone());

        // elect_leader(3) is authority 3, which no block links to in the star DAG.
        let leader = leader_block(&reader, &elector, 3);
        let blocks = blocks_in_rounds(&reader, 3, 5);
        assert!(!scheduler.is_directly_committed(&leader, &blocks));
    }

    /// Wave 2, full mesh: votes and certificates coincide on the decision round
    /// (the special-cased branch) and a quorum includes the leader.
    #[test]
    fn directly_committed_wave2_full_mesh() {
        let committee = committee(4);
        let mut storage = new_storage(&committee);
        build_dag(&committee, &mut storage, None, 4);
        let reader = storage.block_reader().clone();
        let elector = LeaderElector::new(committee.len());
        let scheduler = LeaderScheduler::new(&blue_bottle(&committee, 1, 10), committee.clone());

        let leader = leader_block(&reader, &elector, 3);
        let blocks = blocks_in_rounds(&reader, 3, 4);
        assert!(scheduler.is_directly_committed(&leader, &blocks));
    }

    /// Wave 2, degraded: no decision-round block includes the (non-hub) leader.
    #[test]
    fn not_directly_committed_wave2_unsupported() {
        let committee = committee(4);
        let mut storage = new_storage(&committee);
        build_degraded_dag(&committee, &mut storage, 4);
        let reader = storage.block_reader().clone();
        let elector = LeaderElector::new(committee.len());
        let scheduler = LeaderScheduler::new(&blue_bottle(&committee, 1, 10), committee.clone());

        let leader = leader_block(&reader, &elector, 3);
        let blocks = blocks_in_rounds(&reader, 3, 4);
        assert!(!scheduler.is_directly_committed(&leader, &blocks));
    }

    // --- count_direct_commits ---------------------------------------------

    /// A healthy full mesh commits every leader in the window, so the measured
    /// count equals the analytical expectation (commit rate 1.0).
    #[test]
    fn count_direct_commits_full_mesh_single_leader() {
        let committee = committee(4);
        let mut storage = new_storage(&committee);
        build_dag(&committee, &mut storage, None, 12);
        let reader = storage.block_reader().clone();
        let elector = LeaderElector::new(committee.len());
        let scheduler = LeaderScheduler::new(&mysticeti(&committee, 1, 5, 10), committee.clone());

        let anchor = leader_block(&reader, &elector, 12);
        let subdag = scheduler.collect_subdag(&reader, anchor);
        let expected = scheduler.calculate_expected_commits(1);
        assert_eq!(expected, 8);
        assert_eq!(
            scheduler.count_direct_commits(&subdag, &reader, 1) as u64,
            expected
        );
    }

    /// Same, with five leaders per round on a committee of 10 (offsets 0..5
    /// stay distinct so each is a separate leader).
    #[test]
    fn count_direct_commits_full_mesh_multi_leader() {
        let committee = committee(10);
        let mut storage = new_storage(&committee);
        build_dag(&committee, &mut storage, None, 12);
        let reader = storage.block_reader().clone();
        let elector = LeaderElector::new(committee.len());
        let scheduler = LeaderScheduler::new(&mysticeti(&committee, 5, 5, 10), committee.clone());

        let anchor = leader_block(&reader, &elector, 12);
        let subdag = scheduler.collect_subdag(&reader, anchor);
        let expected = scheduler.calculate_expected_commits(5);
        assert_eq!(expected, 40);
        assert_eq!(
            scheduler.count_direct_commits(&subdag, &reader, 5) as u64,
            expected
        );
    }

    /// A degraded DAG commits nothing, for any leader count.
    #[test]
    fn count_direct_commits_degraded_is_zero() {
        let committee = committee(4);
        let mut storage = new_storage(&committee);
        build_degraded_dag(&committee, &mut storage, 12);
        let reader = storage.block_reader().clone();
        let elector = LeaderElector::new(committee.len());
        let scheduler = LeaderScheduler::new(&mysticeti(&committee, 1, 5, 10), committee.clone());

        let anchor = leader_block(&reader, &elector, 12);
        let subdag = scheduler.collect_subdag(&reader, anchor);
        assert_eq!(scheduler.count_direct_commits(&subdag, &reader, 1), 0);
        assert_eq!(scheduler.count_direct_commits(&subdag, &reader, 4), 0);
    }

    // --- update_leader (end-to-end control loop) --------------------------

    /// Healthy network -> additive increase, backoff counter stays reset, and
    /// the reconfiguration round is recorded.
    #[test]
    fn update_leader_increases_when_healthy() {
        let committee = committee(4);
        let mut storage = new_storage(&committee);
        build_dag(&committee, &mut storage, None, 12);
        let reader = storage.block_reader().clone();
        let elector = LeaderElector::new(committee.len());
        let mut scheduler =
            LeaderScheduler::new(&mysticeti(&committee, 1, 5, 10), committee.clone());

        let anchor = leader_block(&reader, &elector, 12);
        assert_eq!(scheduler.update_leader(&reader, anchor), 2);
        assert_eq!(scheduler.number_of_leaders, 2);
        assert_eq!(scheduler.consecutive_decreases, 0);
        assert_eq!(scheduler.last_round, 12);
    }

    /// Additive increase never exceeds `max_leaders_per_round`.
    #[test]
    fn update_leader_caps_at_max() {
        let committee = committee(10);
        let mut storage = new_storage(&committee);
        build_dag(&committee, &mut storage, None, 12);
        let reader = storage.block_reader().clone();
        let elector = LeaderElector::new(committee.len());
        let mut scheduler =
            LeaderScheduler::new(&mysticeti(&committee, 5, 5, 10), committee.clone());

        let anchor = leader_block(&reader, &elector, 12);
        assert_eq!(scheduler.update_leader(&reader, anchor), 5, "capped at max");
        assert_eq!(scheduler.number_of_leaders, 5);
    }

    /// Unhealthy network -> one multiplicative-backoff step (decrement 1) and
    /// the consecutive-decrease counter advances.
    #[test]
    fn update_leader_decreases_when_degraded() {
        let committee = committee(10);
        let mut storage = new_storage(&committee);
        build_degraded_dag(&committee, &mut storage, 12);
        let reader = storage.block_reader().clone();
        let elector = LeaderElector::new(committee.len());
        let mut scheduler =
            LeaderScheduler::new(&mysticeti(&committee, 4, 5, 10), committee.clone());

        let anchor = leader_block(&reader, &elector, 12);
        assert_eq!(scheduler.update_leader(&reader, anchor), 3);
        assert_eq!(scheduler.consecutive_decreases, 1);
        assert_eq!(scheduler.last_round, 12);
    }

    /// Sustained degradation backs off exponentially: 5 -> 4 -> 2 -> 1, with
    /// the decrement doubling (1, 2, 4, 8) each consecutive step.
    #[test]
    fn update_leader_backs_off_exponentially() {
        let committee = committee(10);
        let mut storage = new_storage(&committee);
        build_degraded_dag(&committee, &mut storage, 12);
        let reader = storage.block_reader().clone();
        let elector = LeaderElector::new(committee.len());
        let mut scheduler =
            LeaderScheduler::new(&mysticeti(&committee, 5, 5, 10), committee.clone());

        let anchor = leader_block(&reader, &elector, 12);
        assert_eq!(scheduler.update_leader(&reader, anchor.clone()), 4); // -1
        assert_eq!(scheduler.update_leader(&reader, anchor.clone()), 2); // -2
        assert_eq!(scheduler.update_leader(&reader, anchor.clone()), 1); // -4, floored
        assert_eq!(scheduler.update_leader(&reader, anchor), 1); // -8, floored
        assert_eq!(scheduler.consecutive_decreases, 4);
    }

    /// Backoff never drops below a single leader.
    #[test]
    fn update_leader_floors_at_one() {
        let committee = committee(4);
        let mut storage = new_storage(&committee);
        build_degraded_dag(&committee, &mut storage, 12);
        let reader = storage.block_reader().clone();
        let elector = LeaderElector::new(committee.len());
        let mut scheduler =
            LeaderScheduler::new(&mysticeti(&committee, 1, 5, 10), committee.clone());

        let anchor = leader_block(&reader, &elector, 12);
        assert_eq!(scheduler.update_leader(&reader, anchor.clone()), 1);
        assert_eq!(scheduler.update_leader(&reader, anchor), 1);
    }

    /// Recovery after backoff: the consecutive-decrease counter resets, so the
    /// next healthy window goes back to additive (single-step) increase.
    #[test]
    fn update_leader_resets_backoff_on_recovery() {
        let committee = committee(4);
        let mut storage = new_storage(&committee);
        build_dag(&committee, &mut storage, None, 12);
        let reader = storage.block_reader().clone();
        let elector = LeaderElector::new(committee.len());
        let mut scheduler =
            LeaderScheduler::new(&mysticeti(&committee, 2, 5, 10), committee.clone());
        scheduler.consecutive_decreases = 3; // pretend we had been backing off

        let anchor = leader_block(&reader, &elector, 12);
        assert_eq!(
            scheduler.update_leader(&reader, anchor),
            3,
            "additive, not jumped"
        );
        assert_eq!(scheduler.consecutive_decreases, 0);
    }

    /// When the window is too short to expect any commit, the leader count is
    /// left untouched (and the backoff counter is not disturbed), but the
    /// reconfiguration round still advances.
    #[test]
    fn update_leader_no_change_when_expected_zero() {
        let committee = committee(4);
        let mut storage = new_storage(&committee);
        build_dag(&committee, &mut storage, None, 5);
        let reader = storage.block_reader().clone();
        let elector = LeaderElector::new(committee.len());
        let mut scheduler =
            LeaderScheduler::new(&mysticeti(&committee, 3, 5, 2), committee.clone());
        scheduler.consecutive_decreases = 2;

        let anchor = leader_block(&reader, &elector, 4);
        assert_eq!(scheduler.update_leader(&reader, anchor), 3, "unchanged");
        assert_eq!(scheduler.number_of_leaders, 3);
        assert_eq!(scheduler.consecutive_decreases, 2, "left alone");
        assert_eq!(scheduler.last_round, 4);
    }
}
