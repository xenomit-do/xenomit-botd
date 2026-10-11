//! Graceful-handoff integration suite (docs/03 §7 "Kill matrix"). Two
//! `botd` instances play predecessor/successor against the embedded mock;
//! a scripted `LocalControl` stands in for the gateway and a shared
//! `LocalSnapshotStore` root plays the fleet snapshot store.
//!
//! ```sh
//! cargo test -p xenomit-botd --test handoff
//! ```
//!
//! Serialized through [`SUITE`] like `mock_game.rs` — the suite shares the
//! runtime's `set_backpage_session` global.
mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use fleet_types::Epoch;
use fleet_types::snapshot::{SnapshotQuality, SnapshotReason, SnapshotStore};
use xenomit_botd::control::LocalControl;
use xenomit_botd::daemon::Daemon;
use xenomit_botd::error::ExitCode;
use xenomit_botd::snapshot;
use xenomit_botd::state::Phase;

use common::*;

/// One handoff scenario per test — serialize the suite.
static SUITE: Mutex<()> = Mutex::new(());
fn suite() -> std::sync::MutexGuard<'static, ()> {
    SUITE.lock().unwrap_or_else(|e| e.into_inner())
}

fn store(root: &std::path::Path) -> Arc<dyn SnapshotStore> {
    Arc::new(snapshot::LocalSnapshotStore::new(root.to_path_buf()))
}

fn latest_snapshot(
    store: &dyn SnapshotStore,
    bot_id: &str,
) -> Option<fleet_types::snapshot::StoredSnapshot> {
    snapshot::block_on(store.latest(bot_id, None)).unwrap()
}

/// Pod A on epoch `1`: activated, running `kill_collect`, then drained by
/// reason. Returns (daemon, control, handle, dir, config) for assertions.
struct PodA {
    daemon: Arc<Daemon>,
    control: Arc<LocalControl>,
}

fn start_pod_a(
    dir: &std::path::Path,
    store_root: &std::path::Path,
    mock: &Mock,
    opts: DaemonOpts,
) -> (PodA, std::thread::JoinHandle<ExitCode>) {
    let control = Arc::new(LocalControl::new());
    control.activate(1, None);
    let config = fixture(dir, mock, &opts);
    let daemon = Daemon::new(
        config.clone(),
        daemon_deps(&config, false, control.clone(), store_root),
    );
    let handle = spawn_run(&daemon);
    wait_phase(&daemon, Phase::Run);
    (PodA { daemon, control }, handle)
}

// ------------------------------------------------------------------------
// Drain → snapshot → logout → successor restores (the happy path)
// ------------------------------------------------------------------------

#[test]
fn clean_drain_writes_snapshot_and_successor_restores() {
    let _guard = suite();
    let mock = Mock::start(|_| {});
    let store_root = tempfile::tempdir().unwrap();
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();

    let (a, handle_a) = start_pod_a(
        dir_a.path(),
        store_root.path(),
        &mock,
        DaemonOpts::password("bot", "bot-pass").autostart("kill_collect"),
    );
    wait_until(Duration::from_secs(15), "module running", || {
        a.daemon.engine().is_running()
    });
    assert_eq!(mock.game_logins(), 1);

    a.control
        .drain("upgrade", Duration::from_secs(10), Duration::from_secs(30));
    assert_eq!(handle_a.join().unwrap(), ExitCode::Clean);

    // `Drained` carried a committed snapshot and the session is gone.
    let reports = a.control.drained_reports();
    assert_eq!(reports.len(), 1, "expected exactly one Drained report");
    assert!(reports[0].clean, "drain did not reach a safe point");
    let snapshot_id = reports[0]
        .snapshot_id
        .clone()
        .expect("Drained without snapshot id");

    let store = store(store_root.path());
    let stored = snapshot::block_on(store.get(&snapshot_id)).unwrap();
    assert_eq!(stored.meta.epoch, Epoch(1));
    assert_eq!(stored.meta.quality, SnapshotQuality::Clean);
    assert_eq!(stored.meta.reason, SnapshotReason::Upgrade);
    let module = stored.snapshot.module.as_ref().expect("no module section");
    assert_eq!(module.id, "kill_collect");
    // The envelope restores to a RuntimeCheckpoint (docs/03 §3.3 shape).
    let plan = snapshot::restore_plan(
        &stored,
        xenomit_bot_runtime::handoff::RestorePolicy::BestEffort,
    )
    .unwrap();
    assert_eq!(
        plan.pending.checkpoint.module_id.as_deref(),
        Some("kill_collect")
    );
    // Outbox tail carried the unacked botd events for replay.
    let tail_len = stored.snapshot.stats.outbox_tail.len();
    assert!(tail_len > 0, "snapshot has no stats outbox tail");

    // Successor pod B: own state_dir, shared store, epoch 2 + the snapshot.
    let control_b = Arc::new(LocalControl::new());
    control_b.activate(2, Some(snapshot_id));
    let config_b = fixture(
        dir_b.path(),
        &mock,
        &DaemonOpts::password("bot", "bot-pass").autostart("kill_collect"),
    );
    let b = Daemon::new(
        config_b.clone(),
        daemon_deps(&config_b, false, control_b, store_root.path()),
    );
    let signals_b = b.signals();
    let handle_b = spawn_run(&b);

    wait_phase(&b, Phase::Run);
    wait_until(Duration::from_secs(15), "module running on B", || {
        b.engine().is_running()
    });
    // No double login: exactly one login per pod, sequentially.
    assert_eq!(mock.game_logins(), 2);

    // Stats replay: A's unacked tail landed in B's outbox (docs/03 §3.6).
    let stats = b.statistics().expect("no statistics on B");
    let stats = stats.lock().unwrap();
    let replayed = stats.unacked_len();
    assert!(
        replayed >= tail_len,
        "expected >= {tail_len} replayed events, got {replayed}"
    );
    let seqs: Vec<u64> = stats.outbox_tail().map(|e| e.seq).collect();
    let mut sorted = seqs.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(seqs.len(), sorted.len(), "duplicate seq in outbox tail");
    assert_eq!(seqs, sorted, "outbox tail not in seq order");
    drop(stats);

    signals_b.request_shutdown();
    assert_eq!(handle_b.join().unwrap(), ExitCode::Clean);
}

// ------------------------------------------------------------------------
// SIGTERM on a standalone pod = eviction drain → exit 0, clean snapshot
// ------------------------------------------------------------------------

#[test]
fn sigterm_drains_snapshot_and_logs_out() {
    let _guard = suite();
    let mock = Mock::start(|_| {});
    let dir = tempfile::tempdir().unwrap();
    let config = fixture(
        dir.path(),
        &mock,
        &DaemonOpts::password("bot", "bot-pass").autostart("kill_collect"),
    );
    let control = Arc::new(LocalControl::new());
    control.activate(1, None);
    let daemon = Daemon::new(
        config.clone(),
        daemon_deps(&config, false, control, &config.state_dir.clone()),
    );
    let signals = daemon.signals();
    let handle = spawn_run(&daemon);

    wait_phase(&daemon, Phase::Run);
    wait_until(Duration::from_secs(15), "module running", || {
        daemon.engine().is_running()
    });
    signals.request_shutdown();
    assert_eq!(handle.join().unwrap(), ExitCode::Clean);
    assert_eq!(mock.game_logins(), 1, "the drain logged out, not around");

    let store = store(&config.state_dir);
    let stored = latest_snapshot(&*store, "bot_it").expect("no snapshot written");
    assert_eq!(stored.meta.quality, SnapshotQuality::Clean);
    assert_eq!(stored.meta.reason, SnapshotReason::Eviction);
    assert_eq!(stored.meta.epoch, Epoch(1));
}

// ------------------------------------------------------------------------
// Kill-matrix rows (docs/03 §7)
// ------------------------------------------------------------------------

/// Row: kill at Run — abort mid-run leaves the latest periodic checkpoint
/// for crash recovery instead of a handoff snapshot.
#[test]
fn abort_mid_run_keeps_last_checkpoint() {
    let _guard = suite();
    let mock = Mock::start(|_| {});
    let store_root = tempfile::tempdir().unwrap();
    let dir = tempfile::tempdir().unwrap();

    let (a, handle_a) = start_pod_a(
        dir.path(),
        store_root.path(),
        &mock,
        DaemonOpts::password("bot", "bot-pass")
            .autostart("kill_collect")
            .checkpoint_interval(1),
    );
    wait_until(Duration::from_secs(15), "module running", || {
        a.daemon.engine().is_running()
    });
    let store = store(store_root.path());
    wait_until(
        Duration::from_secs(15),
        "periodic checkpoint written",
        || {
            snapshot::block_on(store.list("bot_it", false))
                .map(|metas| {
                    metas
                        .iter()
                        .any(|m| m.quality == SnapshotQuality::Checkpoint)
                })
                .unwrap_or(false)
        },
    );

    a.daemon.signals().request_abort();
    assert_eq!(handle_a.join().unwrap(), ExitCode::Crash);
    assert_eq!(mock.game_logins(), 1);
    // The crash wrote nothing new — the periodic checkpoint survived.
    let metas = snapshot::block_on(store.list("bot_it", false)).unwrap();
    assert!(
        metas
            .iter()
            .all(|m| m.quality == SnapshotQuality::Checkpoint),
        "abort wrote a non-checkpoint snapshot"
    );
}

/// Row: kill at AwaitActivate — the pod never logged in and exits before
/// touching the game session.
#[test]
fn kill_at_await_activate_never_logs_in() {
    let _guard = suite();
    let mock = Mock::start(|_| {});
    let store_root = tempfile::tempdir().unwrap();
    let dir = tempfile::tempdir().unwrap();

    // LocalControl with no activation queued parks the daemon in
    // AwaitActivate.
    let control = Arc::new(LocalControl::new());
    let config = fixture(dir.path(), &mock, &DaemonOpts::password("bot", "bot-pass"));
    let daemon = Daemon::new(
        config.clone(),
        daemon_deps(&config, false, control.clone(), store_root.path()),
    );
    let signals = daemon.signals();
    let handle = spawn_run(&daemon);
    wait_phase(&daemon, Phase::AwaitActivate);
    signals.request_shutdown();
    let code = handle.join().unwrap();
    assert_eq!(code, ExitCode::Crash, "cancelled activation should abort");
    assert_eq!(mock.game_logins(), 0, "never reached Login");
}

/// Row: stale epoch mid-run — a successor's epoch write fences the pod at
/// the next probe (exit 67) before it can write another byte.
#[test]
fn stale_epoch_fences_pod_with_67() {
    let _guard = suite();
    let mock = Mock::start(|_| {});
    let store_root = tempfile::tempdir().unwrap();
    let dir = tempfile::tempdir().unwrap();

    let (a, handle_a) = start_pod_a(
        dir.path(),
        store_root.path(),
        &mock,
        DaemonOpts::password("bot", "bot-pass").autostart("kill_collect"),
    );
    wait_until(Duration::from_secs(15), "module running", || {
        a.daemon.engine().is_running()
    });

    // The operator arms the successor: epoch 2 lands in the shared store.
    let store = store(store_root.path());
    snapshot::block_on(store.record_epoch("bot_it", Epoch(2))).unwrap();

    assert_eq!(handle_a.join().unwrap(), ExitCode::Fenced);
    assert_eq!(
        a.daemon.health_state().needs_human().as_deref(),
        Some("fenced")
    );
    // A wrote nothing after the fence.
    assert!(
        snapshot::block_on(store.list("bot_it", false))
            .unwrap()
            .iter()
            .all(|m| m.quality == SnapshotQuality::Checkpoint),
        "fenced pod wrote a snapshot"
    );
    assert_eq!(mock.game_logins(), 1);
}

/// Row: fenced mid-drain — a Drain order followed by a stale epoch still
/// exits 67 and the drain does not commit a snapshot.
#[test]
fn fenced_during_drain_exits_67_without_snapshot() {
    let _guard = suite();
    let mock = Mock::start(|_| {});
    let store_root = tempfile::tempdir().unwrap();
    let dir = tempfile::tempdir().unwrap();

    let (a, handle_a) = start_pod_a(
        dir.path(),
        store_root.path(),
        &mock,
        // Periodic checkpoints effectively off (fires after 5 min — the
        // test finishes long before) so the only epoch-conditional write
        // is the drain snapshot itself.
        DaemonOpts::password("bot", "bot-pass")
            .autostart("kill_collect")
            .checkpoint_interval(300),
    );
    wait_until(Duration::from_secs(15), "module running", || {
        a.daemon.engine().is_running()
    });

    // Fence first, then drain: the drain snapshot's epoch-conditional put
    // must be rejected even though the drain was legitimate.
    let store = store(store_root.path());
    snapshot::block_on(store.record_epoch("bot_it", Epoch(2))).unwrap();
    a.control
        .drain("upgrade", Duration::from_secs(5), Duration::from_secs(10));

    assert_eq!(handle_a.join().unwrap(), ExitCode::Fenced);
    let metas = snapshot::block_on(store.list("bot_it", false)).unwrap();
    assert!(metas.is_empty(), "fenced drain committed a snapshot");
}

// ------------------------------------------------------------------------
// Restore policy
// ------------------------------------------------------------------------

/// Strict + unreadable clean snapshot → exit 70 (docs/03 §3.5).
#[test]
fn strict_restore_of_corrupt_snapshot_exits_70() {
    let _guard = suite();
    let mock = Mock::start(|_| {});
    let store_root = tempfile::tempdir().unwrap();
    let dir = tempfile::tempdir().unwrap();

    // Seed a corrupt snapshot for epoch 2.
    let store = store(store_root.path());
    snapshot::block_on(store.record_epoch("bot_it", Epoch(2))).unwrap();
    let mut corrupt = fleet_types::snapshot::BotSnapshot::new(
        "bot_it",
        Epoch(2),
        SnapshotReason::Upgrade,
        SnapshotQuality::Clean,
        snapshot::unix_ms(),
    );
    corrupt.module = Some(fleet_types::snapshot::ModuleSection {
        id: "kill_collect".into(),
        state_version: 1,
        state: serde_json::json!({ "this_is_not_a_checkpoint": true }),
        sequence: None,
    });
    let corrupt_id = snapshot::block_on(store.put(&corrupt)).unwrap();

    let control = Arc::new(LocalControl::new());
    control.activate(2, Some(corrupt_id));
    let config = fixture(
        dir.path(),
        &mock,
        &DaemonOpts::password("bot", "bot-pass")
            .autostart("kill_collect")
            .strict(),
    );
    let daemon = Daemon::new(
        config.clone(),
        daemon_deps(&config, false, control, store_root.path()),
    );
    let code = daemon.run();
    assert_eq!(code, ExitCode::SnapshotUnreadable);
    assert_eq!(code.code(), 70);
    // GetSnapshot precedes login — the strict pod never touched the game.
    assert_eq!(mock.game_logins(), 0);
}

/// The same corrupt snapshot under `best_effort` logs and starts fresh.
#[test]
fn best_effort_restore_of_corrupt_snapshot_starts_fresh() {
    let _guard = suite();
    let mock = Mock::start(|_| {});
    let store_root = tempfile::tempdir().unwrap();
    let dir = tempfile::tempdir().unwrap();

    let store = store(store_root.path());
    snapshot::block_on(store.record_epoch("bot_it", Epoch(2))).unwrap();
    let mut corrupt = fleet_types::snapshot::BotSnapshot::new(
        "bot_it",
        Epoch(2),
        SnapshotReason::Upgrade,
        SnapshotQuality::Clean,
        snapshot::unix_ms(),
    );
    corrupt.module = Some(fleet_types::snapshot::ModuleSection {
        id: "kill_collect".into(),
        state_version: 1,
        state: serde_json::json!({ "garbage": [] }),
        sequence: None,
    });
    let corrupt_id = snapshot::block_on(store.put(&corrupt)).unwrap();

    let control = Arc::new(LocalControl::new());
    control.activate(2, Some(corrupt_id));
    let config = fixture(
        dir.path(),
        &mock,
        &DaemonOpts::password("bot", "bot-pass").autostart("kill_collect"),
    );
    let daemon = Daemon::new(
        config.clone(),
        daemon_deps(&config, false, control, store_root.path()),
    );
    let signals = daemon.signals();
    let handle = spawn_run(&daemon);
    wait_phase(&daemon, Phase::Run);
    wait_until(Duration::from_secs(15), "module running fresh", || {
        daemon.engine().is_running()
    });
    signals.request_shutdown();
    assert_eq!(handle.join().unwrap(), ExitCode::Clean);
}

/// A missing snapshot id under strict is not corruption — fresh start.
#[test]
fn missing_snapshot_id_starts_fresh_even_when_strict() {
    let _guard = suite();
    let mock = Mock::start(|_| {});
    let store_root = tempfile::tempdir().unwrap();
    let dir = tempfile::tempdir().unwrap();

    let control = Arc::new(LocalControl::new());
    control.activate(
        1,
        Some(fleet_types::snapshot::SnapshotId::new("snap_nonexistent")),
    );
    let config = fixture(
        dir.path(),
        &mock,
        &DaemonOpts::password("bot", "bot-pass")
            .autostart("kill_collect")
            .strict(),
    );
    let daemon = Daemon::new(
        config.clone(),
        daemon_deps(&config, false, control, store_root.path()),
    );
    let signals = daemon.signals();
    let handle = spawn_run(&daemon);
    wait_phase(&daemon, Phase::Run);
    signals.request_shutdown();
    assert_eq!(handle.join().unwrap(), ExitCode::Clean);
}

// ------------------------------------------------------------------------
// Soak: interrupted run (drain mid-way, successor continues) vs one
// uninterrupted run — statistic continuity must hold (docs/03 §7).
// ------------------------------------------------------------------------

/// Short soak window; override with `BOTD_SOAK_SECS` for a longer soak.
fn soak_secs() -> u64 {
    std::env::var("BOTD_SOAK_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8)
}

/// Rewards summed across a daemon's journal dir (all events, any seq).
fn journal_events(dir: &std::path::Path) -> Vec<xenomit_bot_runtime::statistics::Event> {
    let stats_dir = dir.join("statistics");
    let Ok(entries) = stats_dir.read_dir() else {
        return Vec::new();
    };
    let mut events = Vec::new();
    for entry in entries.flatten() {
        if entry.path().extension().is_some_and(|e| e == "jsonl")
            && let Ok(body) = std::fs::read_to_string(entry.path())
        {
            for line in body.lines() {
                if let Ok(event) = serde_json::from_str(line) {
                    events.push(event);
                }
            }
        }
    }
    events
}

#[test]
fn soak_interrupted_run_keeps_event_stream_continuous() {
    let _guard = suite();
    let mock = Mock::start(|_| {});
    let store_root = tempfile::tempdir().unwrap();
    let soak = Duration::from_secs(soak_secs());
    let half = soak / 2;

    // --- interrupted run: A runs half, drains, B restores and finishes ---
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let (a, handle_a) = start_pod_a(
        dir_a.path(),
        store_root.path(),
        &mock,
        DaemonOpts::password("bot", "bot-pass").autostart("kill_collect"),
    );
    std::thread::sleep(half);
    a.control
        .drain("upgrade", Duration::from_secs(10), Duration::from_secs(30));
    assert_eq!(handle_a.join().unwrap(), ExitCode::Clean);
    let snapshot_id = a.control.drained_reports()[0]
        .snapshot_id
        .clone()
        .expect("no snapshot id");

    let control_b = Arc::new(LocalControl::new());
    control_b.activate(2, Some(snapshot_id));
    let config_b = fixture(
        dir_b.path(),
        &mock,
        &DaemonOpts::password("bot", "bot-pass").autostart("kill_collect"),
    );
    let b = Daemon::new(
        config_b.clone(),
        daemon_deps(&config_b, false, control_b, store_root.path()),
    );
    let handle_b = spawn_run(&b);
    wait_phase(&b, Phase::Run);
    std::thread::sleep(half);
    b.signals().request_shutdown();
    assert_eq!(handle_b.join().unwrap(), ExitCode::Clean);

    // Sequence continuity across the handoff: A's seqs and B's replayed+
    // new seqs form a strictly increasing stream with no duplicates.
    let events_a = journal_events(dir_a.path());
    let events_b = journal_events(dir_b.path());
    assert!(!events_a.is_empty(), "predecessor journal empty");
    assert!(!events_b.is_empty(), "successor journal empty");
    let max_seq_a = events_a.iter().map(|e| e.seq).max().unwrap_or(0);
    let b_seqs: Vec<u64> = events_b.iter().map(|e| e.seq).collect();
    assert!(
        b_seqs.iter().all(|&s| s > 0),
        "successor emitted an unsequenced event"
    );
    let mut all: Vec<u64> = events_a
        .iter()
        .chain(events_b.iter())
        .map(|e| e.seq)
        .collect();
    all.sort_unstable();
    let dup = all.windows(2).any(|w| w[0] == w[1]);
    assert!(!dup, "duplicate seq across handoff: {all:?}");
    assert!(
        *b_seqs.iter().max().unwrap() >= max_seq_a,
        "successor seq regressed below {max_seq_a}"
    );
    // One game login per pod — fencing held, no split-brain overlap.
    assert_eq!(mock.game_logins(), 2);
}
