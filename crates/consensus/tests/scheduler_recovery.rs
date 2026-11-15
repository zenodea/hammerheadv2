// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::num::NonZeroUsize;

use consensus::{committer::Committer, protocol::ConsensusProtocol};
use dag::{
    authority::Authority,
    consensus::{DagConsensus, LeaderStatus},
    storage::Storage,
    test_util::{build_dag, build_dag_layer, committee},
};

#[test]
fn replay_restores_scheduling_across_increases_and_decreases() {
    let committee = committee(4);
    let mut storage = Storage::new_for_test(&committee);
    let mut parents = build_dag(&committee, &mut storage, None, 0);
    let spec = ConsensusProtocol::Mysticeti {
        leader_count: NonZeroUsize::new(1).unwrap(),
        adaptive_scheduler: true,
        max_leaders_per_round: NonZeroUsize::new(3).unwrap(),
        scheduler_interval: 5,
    };
    let mut running = Committer::new_for_test(&committee, &storage, &spec);
    let mut cursor = None;
    let mut committed = Vec::new();
    let mut previous_count = 1;
    let mut saw_increase = false;
    let mut saw_decrease = false;

    for round in 1..=80 {
        // One silent validator remains within the four-member committee's fault budget.
        let authors = if (20..50).contains(&round) { 3 } else { 4 };
        parents = build_dag_layer(
            (0..authors)
                .map(|author| (Authority::from(author as usize), parents.clone()))
                .collect(),
            &mut storage,
        );
        let mut restarted = Committer::new_for_test(&committee, &storage, &spec);
        restarted.recover_scheduler(committed.iter().copied());
        assert_eq!(restarted.leaders_per_round(), running.leaders_per_round());

        let uninterrupted = running.try_commit(cursor).collect::<Vec<_>>();
        let recovered = restarted.try_commit(cursor).collect::<Vec<_>>();
        assert_eq!(format!("{uninterrupted:?}"), format!("{recovered:?}"));
        assert_eq!(restarted.leaders_per_round(), running.leaders_per_round());
        if let Some(last) = uninterrupted.last() {
            cursor = Some((last.round(), last.authority()));
        }
        committed.extend(uninterrupted.into_iter().filter_map(|status| match status {
            LeaderStatus::DirectCommit(block) | LeaderStatus::IndirectCommit(block) => {
                Some(*block.reference())
            }
            _ => None,
        }));
        let count = running.leaders_per_round();
        saw_increase |= count > previous_count;
        saw_decrease |= count < previous_count;
        previous_count = count;
    }
    assert!(saw_increase, "scenario did not exercise an increase");
    assert!(saw_decrease, "scenario did not exercise backoff");
}
