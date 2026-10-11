//! Localhost health endpoints (`docs/02-headless-bot.md` §2.5):
//!
//! - `GET /startupz` — 200 once the daemon is armed (secrets resolved,
//!   activation granted); 503 with the current phase before that.
//! - `GET /readyz` — 200 while a session is attached and the data plane is
//!   live; 503 during login/attach/drain or after a `needs_human` failure.
//! - `GET /livez` — 200 unless the supervisor or an expected module-worker
//!   heartbeat stalled for more than `stall` (default 60 s).
//!
//! Minimal HTTP/1.1 on Tokio — one connection, one request, `Connection:
//! close`. No Prometheus endpoint for phase 1a.
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::heartbeat::Heartbeat;
use crate::state::Phase;

const MAX_REQUEST: usize = 8 * 1024;
const READ_TIMEOUT: Duration = Duration::from_secs(5);
/// `/livez` fails when an expected heartbeat is older than this (§2.5).
pub const DEFAULT_STALL: Duration = Duration::from_secs(60);

/// Mutable daemon state the probes read.
pub struct HealthState {
    phase: Mutex<Phase>,
    startup_ok: AtomicBool,
    ready: AtomicBool,
    needs_human: Mutex<Option<String>>,
    last_error: Mutex<Option<String>>,
    /// Actual bind once [`spawn`] succeeds — `127.0.0.1:0` resolves here so
    /// tests and supervisors can learn the chosen port.
    bound_addr: Mutex<Option<SocketAddr>>,
    heartbeat: Arc<Heartbeat>,
    stall: Duration,
}

impl HealthState {
    pub fn new(heartbeat: Arc<Heartbeat>) -> Self {
        Self {
            phase: Mutex::new(Phase::Boot),
            startup_ok: AtomicBool::new(false),
            ready: AtomicBool::new(false),
            needs_human: Mutex::new(None),
            last_error: Mutex::new(None),
            bound_addr: Mutex::new(None),
            heartbeat,
            stall: DEFAULT_STALL,
        }
    }

    pub fn with_stall(mut self, stall: Duration) -> Self {
        self.stall = stall;
        self
    }

    pub fn phase(&self) -> Phase {
        self.phase.lock().map(|p| *p).unwrap_or(Phase::Boot)
    }

    pub fn set_phase(&self, phase: Phase) {
        if let Ok(mut current) = self.phase.lock() {
            *current = phase;
        }
    }

    /// `/startupz` flips to 200 at Arm and stays — arm happened at some point.
    pub fn mark_started(&self) {
        self.startup_ok.store(true, Ordering::Relaxed);
    }

    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Relaxed);
    }

    pub fn set_needs_human(&self, kind: Option<String>) {
        if let Ok(mut slot) = self.needs_human.lock() {
            *slot = kind;
        }
    }

    pub fn needs_human(&self) -> Option<String> {
        self.needs_human.lock().ok().and_then(|s| s.clone())
    }

    pub fn set_last_error(&self, error: Option<String>) {
        if let Ok(mut slot) = self.last_error.lock() {
            *slot = error;
        }
    }

    pub fn heartbeat(&self) -> &Arc<Heartbeat> {
        &self.heartbeat
    }

    /// Bound probe address after [`spawn`]; `None` until the server binds.
    pub fn bound_addr(&self) -> Option<SocketAddr> {
        self.bound_addr.lock().ok().and_then(|slot| *slot)
    }

    pub(crate) fn set_bound_addr(&self, addr: SocketAddr) {
        if let Ok(mut slot) = self.bound_addr.lock() {
            *slot = Some(addr);
        }
    }
}

/// Pure probe decision — unit-testable without sockets.
pub fn respond(state: &HealthState, path: &str) -> (u16, Value) {
    let phase = state.phase();
    match path {
        "/startupz" => {
            if state.startup_ok.load(Ordering::Relaxed) {
                (200, json!({ "status": "ok", "phase": phase.to_string() }))
            } else {
                (
                    503,
                    json!({ "status": "starting", "phase": phase.to_string() }),
                )
            }
        }
        "/readyz" => {
            if state.ready.load(Ordering::Relaxed) {
                (
                    200,
                    json!({ "status": "ready", "phase": phase.to_string() }),
                )
            } else {
                let needs_human = state.needs_human.lock().ok().and_then(|s| s.clone());
                let last_error = state.last_error.lock().ok().and_then(|s| s.clone());
                (
                    503,
                    json!({
                        "status": "not_ready",
                        "phase": phase.to_string(),
                        "needs_human": needs_human,
                        "error": last_error,
                    }),
                )
            }
        }
        "/livez" => {
            let stall_ms = state.stall.as_millis() as u64;
            if state.heartbeat.supervisor_stalled(stall_ms) {
                (
                    503,
                    json!({ "status": "stalled", "reason": "supervisor heartbeat" }),
                )
            } else if state.heartbeat.worker_stalled(stall_ms) {
                (
                    503,
                    json!({ "status": "stalled", "reason": "worker heartbeat" }),
                )
            } else {
                (200, json!({ "status": "alive" }))
            }
        }
        _ => (404, json!({ "status": "not_found" })),
    }
}

/// Bind `bind` and serve probes on `runtime`; returns the bound address
/// (useful with port 0 in tests).
pub fn spawn(
    runtime: &tokio::runtime::Handle,
    bind: SocketAddr,
    state: Arc<HealthState>,
) -> std::io::Result<SocketAddr> {
    let listener = runtime.block_on(TcpListener::bind(bind))?;
    let bound = listener.local_addr()?;
    state.set_bound_addr(bound);
    runtime.spawn(async move {
        while let Ok((socket, _peer)) = listener.accept().await {
            let state = state.clone();
            tokio::spawn(async move {
                let _ = serve(socket, state).await;
            });
        }
    });
    Ok(bound)
}

async fn serve(mut socket: TcpStream, state: Arc<HealthState>) -> std::io::Result<()> {
    let mut buffer = Vec::with_capacity(512);
    let read = tokio::time::timeout(READ_TIMEOUT, async {
        let mut chunk = [0u8; 1024];
        loop {
            let n = socket.read(&mut chunk).await?;
            if n == 0 {
                return Ok::<_, std::io::Error>(());
            }
            buffer.extend_from_slice(&chunk[..n]);
            if buffer.windows(4).any(|w| w == b"\r\n\r\n") || buffer.len() >= MAX_REQUEST {
                return Ok(());
            }
        }
    })
    .await;
    if read.is_err() {
        return Ok(());
    }
    let request = String::from_utf8_lossy(&buffer);
    let path = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/")
        .split('?')
        .next()
        .unwrap_or("/");
    let (status, body) = respond(&state, path);
    let body = serde_json::to_string(&body).unwrap_or_else(|_| "{}".into());
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        _ => "Service Unavailable",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    socket.write_all(response.as_bytes()).await?;
    socket.shutdown().await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> Arc<HealthState> {
        Arc::new(HealthState::new(Arc::new(Heartbeat::new())))
    }

    #[test]
    fn startupz_waits_for_arm() {
        let state = state();
        let (code, body) = respond(&state, "/startupz");
        assert_eq!(code, 503);
        assert_eq!(body["status"], "starting");
        state.mark_started();
        let (code, body) = respond(&state, "/startupz");
        assert_eq!(code, 200);
        assert_eq!(body["status"], "ok");
    }

    #[test]
    fn readyz_reports_needs_human_and_last_error() {
        let state = state();
        state.set_needs_human(Some("captcha".into()));
        state.set_last_error(Some("captcha wall".into()));
        let (code, body) = respond(&state, "/readyz");
        assert_eq!(code, 503);
        assert_eq!(body["needs_human"], "captcha");
        assert_eq!(body["error"], "captcha wall");
        state.set_needs_human(None);
        state.set_ready(true);
        let (code, body) = respond(&state, "/readyz");
        assert_eq!(code, 200);
        assert_eq!(body["status"], "ready");
    }

    #[test]
    fn livez_tracks_supervisor_and_worker_stalls() {
        let heartbeat = Arc::new(Heartbeat::new());
        let state =
            Arc::new(HealthState::new(heartbeat.clone()).with_stall(Duration::from_millis(1)));
        // A fresh heartbeat is alive even with a 1 ms stall window.
        let (code, _) = respond(&state, "/livez");
        assert_eq!(code, 200);
        // Push the supervisor heartbeat into the past.
        heartbeat.supervisor_ms.store(1, Ordering::Relaxed);
        let (code, body) = respond(&state, "/livez");
        assert_eq!(code, 503);
        assert_eq!(body["reason"], "supervisor heartbeat");
        // Supervisor recovers; an expected worker that never beat stalls.
        heartbeat.beat_supervisor();
        heartbeat.set_worker_expected(true);
        heartbeat.worker_ms.store(1, Ordering::Relaxed);
        let (code, body) = respond(&state, "/livez");
        assert_eq!(code, 503);
        assert_eq!(body["reason"], "worker heartbeat");
    }

    #[test]
    fn unknown_paths_are_404() {
        let (code, _) = respond(&state(), "/metrics");
        assert_eq!(code, 404);
    }
}
