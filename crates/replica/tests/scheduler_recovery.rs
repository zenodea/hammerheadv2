// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use consensus::protocol::ConsensusProtocol;
use dag::{authority::Authority, context::TokioCtx, metrics::Metrics, storage::CommitData};
use replica::{
    builder::{ReplicaBuilder, StorageKind},
    config::{PrivateReplicaConfig, PublicReplicaConfig},
    result::Outcome,
};
use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::{Arc, mpsc},
    thread,
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

fn launch(
    i: usize,
    public: PublicReplicaConfig,
    root: &Path,
    metrics: Arc<Metrics>,
) -> (oneshot::Sender<()>, thread::JoinHandle<Vec<CommitData>>) {
    let (stop, stopped) = oneshot::channel();
    let (ready, ready_rx) = mpsc::channel();
    let root: PathBuf = root.into();
    let handle = thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let history = rt.block_on(async move {
            let private = PrivateReplicaConfig::new_for_benchmarks(&root, 4).remove(i);
            let replica = ReplicaBuilder::new(Authority::from(i), public, private)
                .with_storage(StorageKind::Wal(root.join(format!("node-{i}.wal"))))
                .with_metrics(metrics.clone())
                .build()
                .run::<TokioCtx>()
                .await
                .unwrap();
            ready.send(metrics.collect().leaders_per_round()).unwrap();
            let _ = stopped.await;
            let syncer = replica.shutdown().await;
            syncer.core().wal_syncer().sync().unwrap();
            syncer.core().storage().iter_commits().collect::<Vec<_>>()
        });
        rt.shutdown_timeout(Duration::from_secs(1));
        history
    });
    let initial = ready_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("replica startup");
    println!("START node={i} leaders={initial}");
    (stop, handle)
}

fn scenario(adaptive: bool, offset: u16) {
    let temp = tempfile::tempdir().unwrap();
    let mut public = PublicReplicaConfig::new_for_tests(4).with_port_offset(offset);
    public.parameters.consensus = ConsensusProtocol::Mysticeti {
        leader_count: NonZeroUsize::new(1).unwrap(),
        adaptive_scheduler: adaptive,
        max_leaders_per_round: NonZeroUsize::new(3).unwrap(),
        scheduler_interval: 5,
    };
    let mut metrics = vec![];
    let mut nodes = vec![];
    for i in 0..4 {
        let m = Metrics::new_for_test(4);
        nodes.push(Some(launch(i, public.clone(), temp.path(), m.clone())));
        metrics.push(m);
    }
    if adaptive {
        let deadline = Instant::now() + Duration::from_secs(20);
        while !metrics.iter().all(|m| m.collect().leaders_per_round() == 3) {
            assert!(Instant::now() < deadline, "all replicas must adapt");
            thread::sleep(Duration::from_millis(50));
        }
    } else {
        thread::sleep(Duration::from_secs(3));
    }
    let (stop, join) = nodes[0].take().unwrap();
    let _ = stop.send(());
    let before = join.join().unwrap();
    println!(
        "RESULT adaptive={adaptive} before_count={} before_last={:?} leaders={}",
        before.len(),
        before.last().map(|c| c.leader),
        metrics[0].collect().leaders_per_round()
    );
    let fresh = Metrics::new_for_test(4);
    metrics[0] = fresh.clone();
    nodes[0] = Some(launch(0, public, temp.path(), fresh));
    thread::sleep(Duration::from_secs(6));
    let mut histories = vec![];
    for node in nodes {
        let (stop, join) = node.unwrap();
        let _ = stop.send(());
        histories.push(join.join().unwrap());
    }
    let outcome = Outcome::from(histories.clone());
    println!(
        "RESULT adaptive={adaptive} outcome={outcome:?} lengths={:?}",
        histories.iter().map(Vec::len).collect::<Vec<_>>()
    );
    let mut mismatch = None;
    for (index, (a, b)) in histories[0].iter().zip(&histories[1]).enumerate() {
        if a.leader != b.leader || a.sub_dag != b.sub_dag {
            mismatch = Some(index);
            println!(
                "RESULT adaptive={adaptive} mismatch_index={index} restarted={:?} uninterrupted={:?} subdag_equal={}",
                a.leader,
                b.leader,
                a.sub_dag == b.sub_dag
            );
            break;
        }
    }
    if let Some(index) = mismatch {
        assert!(index >= before.len(), "mismatch predates restart");
    }
    assert!(
        histories[0].len() > before.len(),
        "restarted replica made no progress"
    );
    assert_eq!(outcome, Outcome::Pass, "restart must preserve agreement");
}
#[test]
fn static_control() {
    scenario(false, 2200);
}
#[test]
fn adaptive_wal_restart() {
    scenario(true, 3200);
}
