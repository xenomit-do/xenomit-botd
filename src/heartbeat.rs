//! Liveness bookkeeping: the supervisor heartbeats every tick; the module
//! worker heartbeats through [`HeartbeatClient`], a `GameClient` decorator
//! that beats on `status()`/`snapshot()` — the calls the run loop makes
//! every iteration. `/livez` fails when an expected heartbeat stalls.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use xenomit_game_api::{
    ActionResult, ClientStatus, FunctionCall, GameClient, GameSnapshot, Inspection,
    Result as ApiResult,
};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Shared heartbeat timestamps. `0` means "never beat" → treated as stalled
/// once the corresponding `*_expected` flag is set.
pub struct Heartbeat {
    pub(crate) supervisor_ms: AtomicU64,
    pub(crate) worker_ms: AtomicU64,
    /// Worker beats only count while a module is supposed to run; the
    /// daemon clears this before stopping the engine and sets it after a
    /// successful `start`.
    worker_expected: AtomicBool,
}

impl Default for Heartbeat {
    fn default() -> Self {
        Self::new()
    }
}

impl Heartbeat {
    pub fn new() -> Self {
        Self {
            supervisor_ms: AtomicU64::new(now_ms()),
            worker_ms: AtomicU64::new(0),
            worker_expected: AtomicBool::new(false),
        }
    }

    pub fn beat_supervisor(&self) {
        self.supervisor_ms.store(now_ms(), Ordering::Relaxed);
    }

    pub fn beat_worker(&self) {
        self.worker_ms.store(now_ms(), Ordering::Relaxed);
    }

    pub fn set_worker_expected(&self, expected: bool) {
        self.worker_expected.store(expected, Ordering::Relaxed);
        if expected {
            // A fresh module just started; give it a full stall window.
            self.beat_worker();
        }
    }

    /// `true` when the supervisor loop itself stopped ticking.
    pub fn supervisor_stalled(&self, threshold_ms: u64) -> bool {
        now_ms().saturating_sub(self.supervisor_ms.load(Ordering::Relaxed)) > threshold_ms
    }

    /// `true` when a module is expected to run but its heartbeat is stale.
    pub fn worker_stalled(&self, threshold_ms: u64) -> bool {
        if !self.worker_expected.load(Ordering::Relaxed) {
            return false;
        }
        now_ms().saturating_sub(self.worker_ms.load(Ordering::Relaxed)) > threshold_ms
    }
}

/// `GameClient` decorator that beats [`Heartbeat::beat_worker`] on the calls
/// the behavior run loop makes every iteration (`status`, `snapshot`).
/// All other calls delegate untouched.
pub struct HeartbeatClient {
    inner: Arc<dyn GameClient>,
    heartbeat: Arc<Heartbeat>,
}

impl HeartbeatClient {
    pub fn new(inner: Arc<dyn GameClient>, heartbeat: Arc<Heartbeat>) -> Self {
        Self { inner, heartbeat }
    }
}

impl GameClient for HeartbeatClient {
    fn feature_state(&self) -> ApiResult<xenomit_game_api::ClientFeatureState> {
        self.inner.feature_state()
    }

    fn poll_events(&self, after_sequence: u64) -> ApiResult<xenomit_game_api::ClientEventBatch> {
        self.inner.poll_events(after_sequence)
    }

    fn game_command(&self, command: &xenomit_game_api::GameCommand) -> ApiResult<ActionResult> {
        self.inner.game_command(command)
    }

    fn status(&self) -> ApiResult<ClientStatus> {
        let status = self.inner.status();
        if status.is_ok() {
            self.heartbeat.beat_worker();
        }
        status
    }

    fn ping(&self) -> ApiResult<()> {
        self.inner.ping()
    }

    fn snapshot(&self, target: Option<&str>) -> ApiResult<GameSnapshot> {
        let snapshot = self.inner.snapshot(target);
        if snapshot.is_ok() {
            self.heartbeat.beat_worker();
        }
        snapshot
    }

    fn inspect(&self, target: &str) -> ApiResult<Inspection> {
        self.inner.inspect(target)
    }

    fn call_function(&self, call: &FunctionCall) -> ApiResult<ActionResult> {
        self.inner.call_function(call)
    }

    fn read_live_frame(&self, frame: &mut xenomit_game_api::live::LiveFrame) -> bool {
        self.inner.read_live_frame(frame)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xenomit_game_api::ApiError;

    struct StubClient;

    impl GameClient for StubClient {
        fn status(&self) -> ApiResult<ClientStatus> {
            Ok(ClientStatus {
                backend: "stub".into(),
                connected: true,
                ready: true,
                details: Default::default(),
            })
        }
        fn ping(&self) -> ApiResult<()> {
            Ok(())
        }
        fn snapshot(&self, _target: Option<&str>) -> ApiResult<GameSnapshot> {
            Err(ApiError::Unavailable("no snapshot".into()))
        }
        fn inspect(&self, _target: &str) -> ApiResult<Inspection> {
            Err(ApiError::Unavailable("no inspect".into()))
        }
        fn call_function(&self, _call: &FunctionCall) -> ApiResult<ActionResult> {
            Err(ApiError::Unavailable("no calls".into()))
        }
    }

    #[test]
    fn worker_beats_through_status_calls() {
        let heartbeat = Arc::new(Heartbeat::new());
        heartbeat.set_worker_expected(true);
        heartbeat.worker_ms.store(0, Ordering::Relaxed);
        let client = HeartbeatClient::new(Arc::new(StubClient), heartbeat.clone());
        assert!(heartbeat.worker_stalled(0));
        client.status().unwrap();
        assert!(!heartbeat.worker_stalled(0));
    }

    #[test]
    fn unexpected_worker_never_stalls() {
        let heartbeat = Heartbeat::new();
        assert!(!heartbeat.worker_stalled(0));
        heartbeat.set_worker_expected(true);
        heartbeat.worker_ms.store(1, Ordering::Relaxed);
        assert!(heartbeat.worker_stalled(60_000));
    }
}
