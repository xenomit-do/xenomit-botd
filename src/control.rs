//! Control-plane abstraction (`docs/04-orchestrator.md`, docs/03 §3.2/§3.7).
//!
//! `StandaloneControl` keeps single-process mode working: no `control` block
//! means the daemon arms, auto-activates at epoch 1 and writes snapshots to
//! the local store only. `GatewayControl` is a clear stub — a configured
//! `control` block fails at Arm instead of silently running unmanaged until
//! the real gRPC stream lands. [`LocalControl`] is the scripted fake the
//! kill-matrix/soak tests drive: it plays the operator's side of the
//! protocol (activate, drain, fence) in-process.
use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use fleet_types::Epoch;
use fleet_types::snapshot::SnapshotId;
use serde_json::Value;

use crate::error::BotdError;
use crate::secrets::SecretString;

/// What the control plane grants at activation.
#[derive(Clone, Debug)]
pub struct Activation {
    /// Fencing epoch of this pod (docs/03 §3.7; first pod = epoch 1).
    pub epoch: Epoch,
    /// Snapshot to restore after attach; `None` = fresh start.
    pub snapshot_id: Option<SnapshotId>,
}

/// A drain order: from the gateway's `Drain` command, or the local mapping
/// of SIGTERM/panel shutdown (docs/03 §3.2 — every trigger is the same path).
#[derive(Clone, Debug)]
pub struct DrainOrder {
    /// `HandoffReason` string (upgrade, eviction, proxy_change, …).
    pub reason: String,
    /// Cooperative window; pauses while `non_interruptible` (docs/03 §3.2).
    pub drain_deadline: Duration,
    /// Hard wall-clock cutoff — the drain concludes degraded past it.
    pub hard_deadline: Duration,
    /// W3C trace context propagated into the drained report.
    pub traceparent: Option<String>,
}

/// Commands the gateway can push mid-run (`GatewayCommand`, fleet.proto).
#[derive(Clone, Debug)]
pub enum ControlCommand {
    /// Reach a safe point, snapshot, flush, log out, exit 0.
    Drain(DrainOrder),
    /// desired_state=stopped: drain without a successor.
    Shutdown { reason: String },
    /// desired_state=paused: drain without a successor.
    Pause { reason: String },
    /// desired_state=running again after a pause that did not drain yet.
    Resume { reason: String },
    /// Hot-applicable full `BotSettings` document at `revision`.
    ApplyConfig { revision: u64, settings: Value },
    /// Our epoch is stale; exit 67 without touching the game.
    Fenced { expected: Epoch, got: Epoch },
}

/// A `Drained` report delivered to the control plane (fleet.proto).
#[derive(Clone, Debug, PartialEq)]
pub struct DrainedReport {
    pub snapshot_id: Option<SnapshotId>,
    /// `clean` — the snapshot was taken at a safe point.
    pub clean: bool,
}

/// Managed-mode seam. Implementations own activation, mid-run commands and
/// fencing. The epoch returned by [`ControlPlane::wait_activate`] is
/// re-checked each supervisor tick through [`ControlPlane::current_epoch`]
/// and on every epoch-conditional snapshot write.
pub trait ControlPlane: Send + Sync {
    /// Arm the control channel (resolve/connect). Runs inside `Arm`.
    fn arm(&self) -> Result<(), BotdError>;
    /// Block until activation is granted or `cancel` requests shutdown.
    fn wait_activate(&self, cancel: &AtomicBool) -> Result<Activation, BotdError>;
    /// Drain queued gateway commands (non-blocking, called once per tick).
    fn poll_commands(&self) -> Vec<ControlCommand> {
        Vec::new()
    }
    /// The bot's current epoch as the control plane records it. `None` =
    /// unknown/not tracked (standalone never fences itself).
    fn current_epoch(&self) -> Option<Epoch> {
        None
    }
    /// Terminal drain report (`Drained` event): snapshot committed, outbox
    /// flushed, logged out — the operator may now activate the successor.
    fn report_drained(&self, _report: DrainedReport) -> Result<(), BotdError> {
        Ok(())
    }
    /// Lifecycle phase report (fleet.proto `PhaseChanged`, observability).
    fn report_phase(&self, _phase: &str, _detail: &str) {}
    /// Report the proxy's observed exit IP (docs/05 §5.4 — egress
    /// identity). Default no-op; [`GatewayControl`] forwards it as a
    /// `MetricAlert` so the operator can correlate egress reputation.
    fn report_exit_ip(&self, _ip: &str) {}
}

/// No `control` block — the daemon is its own control plane. Activates at
/// epoch 1 (docs/03 §3.7: the first pod is epoch 1) and never fences.
pub struct StandaloneControl {
    epoch: AtomicU64,
}

impl Default for StandaloneControl {
    fn default() -> Self {
        Self {
            epoch: AtomicU64::new(0),
        }
    }
}

impl StandaloneControl {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ControlPlane for StandaloneControl {
    fn arm(&self) -> Result<(), BotdError> {
        Ok(())
    }

    fn wait_activate(&self, _cancel: &AtomicBool) -> Result<Activation, BotdError> {
        self.epoch.store(1, Ordering::Relaxed);
        Ok(Activation {
            epoch: Epoch(1),
            snapshot_id: None,
        })
    }

    fn current_epoch(&self) -> Option<Epoch> {
        Some(Epoch(self.epoch.load(Ordering::Relaxed)))
    }
}

// ---------------------------------------------------------------------
// Managed mode: tonic FleetControl stream (docs/04 §4.5)
// ---------------------------------------------------------------------

use std::sync::Arc;

use fleet_types::proto::{
    ClientEvent, GatewayCommand, client_event, fleet_control_client::FleetControlClient,
    gateway_command,
};
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Endpoint;

use xenomit_core::logging::tracing::{info, warn};

use crate::secrets::{SecretError, SecretRef, SecretResolver};
use crate::snapshot::LocalSnapshotStore;
use fleet_types::snapshot::{
    BotSnapshot, SnapshotQuality, SnapshotStore, StoreError, StoredSnapshot,
};
use tokio_stream::StreamExt;

/// Reconnect cadence after a transport failure: doubles to this ceiling.
const RECONNECT_MIN: Duration = Duration::from_millis(250);
const RECONNECT_MAX: Duration = Duration::from_secs(5);
/// Lease renewals ride this interval (`fleet-types` TTL is 30 s).
const LEASE_INTERVAL: Duration = Duration::from_secs(10);
/// How long a snapshot `put` waits for the gateway `Ack`.
const ACK_WAIT: Duration = Duration::from_secs(20);
/// Grace the `Drained` event gets to reach the wire before the process exits.
const DRAIN_FLUSH_WAIT: Duration = Duration::from_secs(3);

/// One queued daemon→gateway event. Snapshot puts carry the waiter their
/// `Ack` resolves — registered only at wire-send so ordering is exact.
enum OutEvent {
    Send(ClientEvent),
    Put(ClientEvent, oneshot::Sender<Result<SnapshotId, String>>),
}

/// Shared daemon↔stream state, alive for the pod's lifetime (survives
/// stream reconnects). The async half reads `out_rx`/`retry`; the daemon
/// side stays fully synchronous.
struct LinkInner {
    bot_id: String,
    /// This pod's fencing epoch (`XENOMIT_EPOCH`); carried on every event.
    epoch: u64,
    out: mpsc::UnboundedSender<OutEvent>,
    /// The receiver lives behind a mutex because the reconnect loop
    /// re-borrows it for each fresh stream.
    out_rx: AsyncMutex<mpsc::UnboundedReceiver<OutEvent>>,
    /// Events pulled from `out` but unsent when the stream died — they go
    /// out before anything newer on the next connection.
    retry: Mutex<VecDeque<OutEvent>>,
    /// Awaited snapshot `Ack`s, in wire order.
    pending_puts: Mutex<VecDeque<oneshot::Sender<Result<SnapshotId, String>>>>,
    /// The `Activate` `wait_activate` hands to the lifecycle.
    activation: Mutex<Option<Activation>>,
    /// `Activate` is idempotent per (bot, epoch): this pod's lifetime is one
    /// epoch, so the first delivery arms the slot and any duplicate — a
    /// gateway retry, a session flap re-delivery — must not overwrite it
    /// mid-login (docs/03 §3.7).
    activated: AtomicBool,
    /// Commands for `poll_commands`.
    inbox: Mutex<VecDeque<ControlCommand>>,
    /// Fencing epoch the gateway reported (`Fenced.expected`), 0 = none.
    fenced: AtomicU64,
    /// Serving epoch learned from `Activate` (0 before activation).
    serving: AtomicU64,
    /// Set on `Fenced`/shutdown — the stream loop exits and stays down.
    closed: AtomicBool,
    /// Successful wire sends — `report_drained` polls it for its flush.
    flush_count: AtomicU64,
}

/// The daemon-side handle to the control stream. `GatewayControl` owns the
/// reconnect task; `GatewayStore` shares the link for snapshot uploads.
#[derive(Clone)]
pub struct GatewayLink {
    inner: Arc<LinkInner>,
}

impl GatewayLink {
    pub fn new(bot_id: impl Into<String>, epoch: u64) -> Self {
        let (out, out_rx) = mpsc::unbounded_channel();
        Self {
            inner: Arc::new(LinkInner {
                bot_id: bot_id.into(),
                epoch,
                out,
                out_rx: AsyncMutex::new(out_rx),
                retry: Mutex::new(VecDeque::new()),
                pending_puts: Mutex::new(VecDeque::new()),
                activation: Mutex::new(None),
                activated: AtomicBool::new(false),
                inbox: Mutex::new(VecDeque::new()),
                fenced: AtomicU64::new(0),
                serving: AtomicU64::new(0),
                closed: AtomicBool::new(false),
                flush_count: AtomicU64::new(0),
            }),
        }
    }

    /// Queue a fire-and-forget event (`LeaseRenew`, `PhaseChanged`, …).
    fn send(&self, event: ClientEvent) {
        let _ = self.inner.out.send(OutEvent::Send(event));
    }

    /// Queue a snapshot event and return the waiter its `Ack` resolves.
    fn put(&self, event: ClientEvent) -> oneshot::Receiver<Result<SnapshotId, String>> {
        let (tx, rx) = oneshot::channel();
        let _ = self.inner.out.send(OutEvent::Put(event, tx));
        rx
    }

    /// `Drain`/`Shutdown`/… the gateway pushed, in order.
    fn take_commands(&self) -> Vec<ControlCommand> {
        self.inner
            .inbox
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
            .collect()
    }
}

/// The `FleetControl` client: a reconnect-with-backoff tonic stream on a
/// dedicated thread, driven through [`GatewayLink`]. `arm()` resolves the
/// bearer token through the daemon's [`SecretResolver`] (a Vault
/// service-account token in production, a file in dev).
pub struct GatewayControl {
    link: GatewayLink,
    endpoint: String,
    token_ref: SecretRef,
    resolver: Arc<dyn SecretResolver>,
    /// Once `arm` spawns the stream task, retries are no-ops.
    started: AtomicBool,
}

impl GatewayControl {
    pub fn new(
        endpoint: String,
        token_ref: SecretRef,
        resolver: Arc<dyn SecretResolver>,
        bot_id: impl Into<String>,
        epoch: u64,
    ) -> Self {
        Self {
            link: GatewayLink::new(bot_id, epoch),
            endpoint,
            token_ref,
            resolver,
            started: AtomicBool::new(false),
        }
    }

    /// The shared link — `GatewayStore` needs it for snapshot uploads.
    pub fn link(&self) -> GatewayLink {
        self.link.clone()
    }
}

impl ControlPlane for GatewayControl {
    fn arm(&self) -> Result<(), BotdError> {
        if self.started.swap(true, Ordering::Relaxed) {
            return Ok(());
        }
        let raw = self
            .resolver
            .resolve(&self.token_ref)
            .map_err(secret_error)?;
        let token = crate::secrets::parse_token(&raw, "control token").map_err(secret_error)?;
        let endpoint = self.endpoint.clone();
        let link = self.link.clone();
        std::thread::Builder::new()
            .name("control-gateway".into())
            .spawn(move || stream_loop(link, endpoint, token))
            .map_err(|e| BotdError::Unavailable(format!("control thread: {e}")))?;
        Ok(())
    }

    fn wait_activate(&self, cancel: &AtomicBool) -> Result<Activation, BotdError> {
        loop {
            if let Some(activation) = self
                .link
                .inner
                .activation
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take()
            {
                return Ok(activation);
            }
            let fenced = self.link.inner.fenced.load(Ordering::Relaxed);
            if fenced > 0 {
                return Err(BotdError::Fenced(format!(
                    "epoch {} fenced by newer epoch {fenced}",
                    self.link.inner.epoch
                )));
            }
            if cancel.load(Ordering::Relaxed) {
                return Err(BotdError::Unavailable(
                    "activation cancelled by shutdown".into(),
                ));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn poll_commands(&self) -> Vec<ControlCommand> {
        self.link.take_commands()
    }

    fn current_epoch(&self) -> Option<Epoch> {
        let epoch = self
            .link
            .inner
            .fenced
            .load(Ordering::Relaxed)
            .max(self.link.inner.serving.load(Ordering::Relaxed));
        (epoch > 0).then_some(Epoch(epoch))
    }

    fn report_drained(&self, report: DrainedReport) -> Result<(), BotdError> {
        self.link.send(ClientEvent {
            event: Some(client_event::Event::Drained(fleet_types::proto::Drained {
                epoch: self.link.inner.epoch,
                snapshot_id: report.snapshot_id.map(|id| id.0).unwrap_or_default(),
                clean: report.clean,
            })),
        });
        // The process exits right after `Drained`; give the writer a
        // bounded window to actually flush it onto the wire.
        let sent = self.link.inner.flush_count.load(Ordering::Relaxed);
        let deadline = std::time::Instant::now() + DRAIN_FLUSH_WAIT;
        while !self.link.inner.closed.load(Ordering::Relaxed)
            && std::time::Instant::now() < deadline
        {
            if self.link.inner.flush_count.load(Ordering::Relaxed) > sent {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        Ok(())
    }

    fn report_phase(&self, phase: &str, detail: &str) {
        let Some(mapped) = map_phase(phase) else {
            return;
        };
        self.link.send(ClientEvent {
            event: Some(client_event::Event::PhaseChanged(
                fleet_types::proto::PhaseChanged {
                    epoch: self.link.inner.epoch,
                    phase: mapped.into(),
                    detail: detail.to_owned(),
                },
            )),
        });
    }

    fn report_exit_ip(&self, ip: &str) {
        self.link.send(ClientEvent {
            event: Some(client_event::Event::MetricAlert(
                fleet_types::proto::MetricAlert {
                    epoch: self.link.inner.epoch,
                    name: "proxy_exit_ip".into(),
                    severity: "info".into(),
                    message: ip.to_owned(),
                },
            )),
        });
    }
}

impl Drop for GatewayControl {
    fn drop(&mut self) {
        self.link.inner.closed.store(true, Ordering::Relaxed);
    }
}

/// daemon `Phase` → `BotPhase` vocabulary the gateway/`BotController`
/// consumes. Stages with no CR-side equivalent (boot, config load,
/// exit-checkpoint writes) are dropped — they're not observable phases.
fn map_phase(phase: &str) -> Option<&'static str> {
    Some(match phase {
        "arm" | "await_activate" => "armed",
        "login" | "attach" => "starting",
        "run" => "running",
        "draining" | "logging_out" => "draining",
        "snapshotting" => "snapshotting",
        "needs_human" => "needs_human",
        "needs_update" => "needs_update",
        "failed" => "failed",
        _ => return None,
    })
}

/// The reconnect loop: connect → run the stream → back off → repeat until
/// `closed` (fenced or shutdown). Runs on its own `current_thread`
/// runtime — the daemon side never sees async.
fn stream_loop(link: GatewayLink, endpoint: String, token: SecretString) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            warn!(%e, "control stream runtime failed to start");
            return;
        }
    };
    runtime.block_on(async move {
        let mut backoff = RECONNECT_MIN;
        while !link.inner.closed.load(Ordering::Relaxed) {
            match stream_session(&link, &endpoint, &token).await {
                Ok(()) => info!("control stream ended"),
                Err(e) => warn!(error = %e, "control stream failed — reconnecting"),
            }
            if link.inner.closed.load(Ordering::Relaxed) {
                return;
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(RECONNECT_MAX);
        }
    });
}

/// One stream's lifetime wrapper: run the session, then fail every
/// un-acked snapshot put — a dead stream can't answer for them.
async fn stream_session(
    link: &GatewayLink,
    endpoint: &str,
    token: &SecretString,
) -> Result<(), String> {
    let result = session_run(link, endpoint, token).await;
    let mut pending = link
        .inner
        .pending_puts
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    for waiter in pending.drain(..) {
        let _ = waiter.send(Err("control stream lost — snapshot unconfirmed".into()));
    }
    result
}

/// One stream's lifetime: connect → Hello → forward `out` events and
/// lease renewals → dispatch gateway commands. Returns on stream end or
/// error; the caller decides whether to reconnect.
async fn session_run(
    link: &GatewayLink,
    endpoint: &str,
    token: &SecretString,
) -> Result<(), String> {
    let (wire_tx, wire_rx) = mpsc::unbounded_channel::<ClientEvent>();
    let stream = UnboundedReceiverStream::new(wire_rx);
    let mut request = tonic::Request::new(stream);
    let bearer = format!("Bearer {}", token.reveal());
    request.metadata_mut().insert(
        "authorization",
        bearer
            .parse()
            .map_err(|_| "control token is not header-safe".to_string())?,
    );
    let channel = Endpoint::new(endpoint.to_owned())
        .map_err(|e| format!("bad control endpoint: {e}"))?
        .connect_timeout(Duration::from_secs(10));
    let mut client = FleetControlClient::connect(channel)
        .await
        .map_err(|e| format!("control connect failed: {e}"))?;
    let mut inbound = client
        .session(request)
        .await
        .map_err(|e| format!("control session failed: {e}"))?
        .into_inner();

    // Hello first — the gateway rejects anything else.
    wire_tx
        .send(ClientEvent {
            event: Some(client_event::Event::Hello(fleet_types::proto::Hello {
                bot_id: link.inner.bot_id.clone(),
                epoch: link.inner.epoch,
                botd_version: env!("CARGO_PKG_VERSION").to_owned(),
                phase: "armed".into(),
                traceparent: String::new(),
            })),
        })
        .map_err(|_| "control wire closed".to_string())?;

    let mut out_rx = link.inner.out_rx.lock().await;
    let mut lease_seq = 0u64;
    let mut lease = tokio::time::interval(LEASE_INTERVAL);
    lease.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        // `retry` events (pulled but unsent on a dead stream) go first.
        let pending_out = link
            .inner
            .retry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pop_front();
        if let Some(ev) = pending_out {
            forward(link, ev, &wire_tx)?;
            continue;
        }
        tokio::select! {
            cmd = inbound.next() => match cmd {
                Some(Ok(cmd)) => {
                    if link.inner.closed.load(Ordering::Relaxed) {
                        return Ok(());
                    }
                    dispatch(link, cmd);
                }
                Some(Err(status)) => {
                    return Err(format!("control stream error: {status}"));
                }
                None => return Ok(()),
            },
            ev = out_rx.recv() => match ev {
                Some(out) => forward(link, out, &wire_tx)?,
                // `out` sender dropped → the daemon is shutting down.
                None => return Ok(()),
            },
            _ = lease.tick() => {
                lease_seq += 1;
                wire_tx
                    .send(ClientEvent {
                        event: Some(client_event::Event::LeaseRenew(
                            fleet_types::proto::LeaseRenew {
                                epoch: link.inner.epoch,
                                lease_seq,
                            },
                        )),
                    })
                    .map_err(|_| "control wire closed".to_string())?;
            }
        }
        if link.inner.closed.load(Ordering::Relaxed) {
            return Ok(());
        }
    }
}

/// Push one queued daemon event onto the wire. Snapshot puts register
/// their `Ack` waiter *at send time* so the FIFO stays exact; a failed
/// send requeues event and waiter for the next stream.
fn forward(
    link: &GatewayLink,
    out: OutEvent,
    wire_tx: &mpsc::UnboundedSender<ClientEvent>,
) -> Result<(), String> {
    let (ev, waiter) = match out {
        OutEvent::Send(ev) => (ev, None),
        OutEvent::Put(ev, w) => (ev, Some(w)),
    };
    let is_put = waiter.is_some();
    if let Some(w) = waiter {
        link.inner
            .pending_puts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push_back(w);
    }
    match wire_tx.send(ev) {
        Ok(()) => {
            link.inner.flush_count.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        Err(send_err) => {
            // Never hit the wire — requeue event (and its waiter, if we
            // just registered one) ahead of everything else.
            let ev = send_err.0;
            let out = if is_put {
                // Ours is the tail — single writer.
                let w = link
                    .inner
                    .pending_puts
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .pop_back()
                    .expect("waiter registered above");
                OutEvent::Put(ev, w)
            } else {
                OutEvent::Send(ev)
            };
            link.inner
                .retry
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push_front(out);
            Err("control wire closed".into())
        }
    }
}

/// Translate a gateway command into the daemon's command vocabulary.
fn dispatch(link: &GatewayLink, cmd: GatewayCommand) {
    use gateway_command::Command as C;
    let inner = &link.inner;
    let mut inbox = inner.inbox.lock().unwrap_or_else(|e| e.into_inner());
    match cmd.command {
        Some(C::Activate(a)) => {
            inner.serving.fetch_max(a.epoch, Ordering::Relaxed);
            // Idempotent per (bot, epoch): the first `Activate` arms the
            // one-shot slot `wait_activate` consumes; a duplicate push must
            // not re-arm it or overwrite the snapshot choice mid-flight.
            if !inner.activated.swap(true, Ordering::Relaxed) {
                *inner.activation.lock().unwrap_or_else(|e| e.into_inner()) = Some(Activation {
                    epoch: Epoch(a.epoch),
                    snapshot_id: (!a.snapshot_id.is_empty())
                        .then(|| SnapshotId::new(a.snapshot_id)),
                });
            }
        }
        Some(C::Drain(d)) => inbox.push_back(ControlCommand::Drain(DrainOrder {
            reason: d.reason,
            drain_deadline: Duration::from_secs(d.drain_deadline_s as u64),
            hard_deadline: Duration::from_secs(d.hard_deadline_s as u64),
            traceparent: (!d.traceparent.is_empty()).then_some(d.traceparent),
        })),
        Some(C::ApplyConfig(a)) => inbox.push_back(ControlCommand::ApplyConfig {
            revision: a.revision,
            settings: serde_json::from_str(&a.settings_json).unwrap_or(Value::Null),
        }),
        Some(C::Pause(p)) => inbox.push_back(ControlCommand::Pause { reason: p.reason }),
        Some(C::Resume(r)) => inbox.push_back(ControlCommand::Resume { reason: r.reason }),
        Some(C::Shutdown(s)) => inbox.push_back(ControlCommand::Shutdown { reason: s.reason }),
        Some(C::Fenced(f)) => {
            inner.fenced.store(f.expected_epoch, Ordering::Relaxed);
            inbox.push_back(ControlCommand::Fenced {
                expected: Epoch(f.expected_epoch),
                got: Epoch(f.got_epoch),
            });
            // No `Ack` is coming — fail the outstanding snapshot puts now
            // so the drain path isn't stalled until `ACK_WAIT` blows.
            for waiter in inner
                .pending_puts
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .drain(..)
            {
                let _ = waiter.send(Err("stream fenced — snapshot unconfirmed".into()));
            }
        }
        Some(C::Ack(a)) => {
            if let Some(waiter) = inner
                .pending_puts
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .pop_front()
            {
                let _ = waiter.send(Ok(SnapshotId::new(a.snapshot_id)));
            }
        }
        None => {}
    }
    if inner.fenced.load(Ordering::Relaxed) > 0 {
        inner.closed.store(true, Ordering::Relaxed);
    }
}

/// `SecretError` → `BotdError`: `Unavailable` is transient (Arm retries),
/// `Invalid` is a config fault.
fn secret_error(error: SecretError) -> BotdError {
    match error {
        SecretError::Unavailable(message) => BotdError::Unavailable(message),
        SecretError::Invalid(message) => BotdError::Config(message),
    }
}

/// [`SnapshotStore`] over the gateway: `put` ships the document as a
/// `Checkpoint`/`PutSnapshot` event and resolves on the `Ack`; reads and
/// retention mirror into a shared [`LocalSnapshotStore`] so `get(id)`
/// works for ids `Activate` carries and cross-process fencing survives.
pub struct GatewayStore {
    link: GatewayLink,
    local: LocalSnapshotStore,
}

impl GatewayStore {
    pub fn new(link: GatewayLink, root: std::path::PathBuf) -> Self {
        Self {
            link,
            local: LocalSnapshotStore::new(root),
        }
    }
}

#[async_trait::async_trait]
impl SnapshotStore for GatewayStore {
    async fn record_epoch(&self, bot_id: &str, epoch: Epoch) -> Result<(), StoreError> {
        self.local.record_epoch(bot_id, epoch).await
    }

    async fn current_epoch(&self, bot_id: &str) -> Result<Option<Epoch>, StoreError> {
        self.local.current_epoch(bot_id).await
    }

    async fn put(&self, snapshot: &BotSnapshot) -> Result<SnapshotId, StoreError> {
        let fenced = self.link.inner.fenced.load(Ordering::Relaxed);
        if fenced > 0 && fenced != self.link.inner.epoch {
            return Err(StoreError::Fenced(fleet_types::epoch::Fenced {
                expected: Epoch(fenced),
                got: Epoch(self.link.inner.epoch),
            }));
        }
        let bytes = snapshot.encode()?;
        let epoch = self.link.inner.epoch;
        let event = match snapshot.quality {
            SnapshotQuality::Checkpoint => ClientEvent {
                event: Some(client_event::Event::Checkpoint(
                    fleet_types::proto::Checkpoint {
                        epoch,
                        snapshot: bytes,
                    },
                )),
            },
            quality => ClientEvent {
                event: Some(client_event::Event::PutSnapshot(
                    fleet_types::proto::PutSnapshot {
                        epoch,
                        quality: match quality {
                            SnapshotQuality::Clean => "clean",
                            _ => "degraded",
                        }
                        .into(),
                        snapshot: bytes,
                    },
                )),
            },
        };
        let rx = self.link.put(event);
        let result = match tokio::time::timeout(ACK_WAIT, rx).await {
            Ok(Ok(result)) => result.map_err(StoreError::Backend)?,
            Ok(Err(_)) => return Err(StoreError::Backend("control ack dropped".into())),
            Err(_) => return Err(StoreError::Backend("control ack timed out".into())),
        };
        // Mirror the gateway's id locally so `get`/`latest` resolve
        // without a second store (successor pods share this root).
        if !result.0.is_empty() {
            self.local.mirror(&result, snapshot)?;
        }
        Ok(result)
    }

    async fn get(&self, id: &SnapshotId) -> Result<StoredSnapshot, StoreError> {
        self.local.get(id).await
    }

    async fn latest(
        &self,
        bot_id: &str,
        quality: Option<fleet_types::snapshot::SnapshotQuality>,
    ) -> Result<Option<StoredSnapshot>, StoreError> {
        self.local.latest(bot_id, quality).await
    }

    async fn list(
        &self,
        bot_id: &str,
        include_deleted: bool,
    ) -> Result<Vec<fleet_types::snapshot::SnapshotMeta>, StoreError> {
        self.local.list(bot_id, include_deleted).await
    }

    async fn soft_delete(&self, bot_id: &str, at_unix_ms: i64) -> Result<(), StoreError> {
        self.local.soft_delete(bot_id, at_unix_ms).await
    }

    async fn undelete(&self, bot_id: &str) -> Result<(), StoreError> {
        self.local.undelete(bot_id).await
    }

    async fn purge(&self, before_unix_ms: i64) -> Result<u64, StoreError> {
        self.local.purge(before_unix_ms).await
    }

    async fn gc(&self, bot_id: &str, keep_clean: usize) -> Result<u64, StoreError> {
        self.local.gc(bot_id, keep_clean).await
    }
}

/// Scripted in-process control plane for tests and local multi-process dev
/// (docs/02 §2.6 `local` backend shape). The test plays the operator: push
/// an `Activation` for `wait_activate` to return, queue `Drain`/`Fenced`
/// commands, observe `Drained` reports.
#[derive(Default)]
pub struct LocalControl {
    /// Activation `wait_activate` returns; `None` parks the daemon in
    /// `AwaitActivate` until the test pushes one.
    pending: Mutex<Option<Activation>>,
    /// Queued `GatewayCommand`s, delivered one tick at a time.
    commands: Mutex<VecDeque<ControlCommand>>,
    /// The operator's view of the bot's current epoch. Bumped by
    /// `set_epoch` when a successor arms — the stale daemon fences itself
    /// on its next probe (exit 67).
    epoch: AtomicU64,
    /// Drained reports received, in order.
    drained: Mutex<Vec<DrainedReport>>,
    /// Phase reports received (`phase` strings).
    phases: Mutex<Vec<String>>,
}

impl LocalControl {
    pub fn new() -> Self {
        Self::default()
    }
    /// Activate the daemon at `epoch` (optionally restoring `snapshot_id`).
    pub fn activate(&self, epoch: u64, snapshot_id: Option<SnapshotId>) {
        self.epoch.store(epoch, Ordering::Relaxed);
        *self.pending.lock().unwrap_or_else(|e| e.into_inner()) = Some(Activation {
            epoch: Epoch(epoch),
            snapshot_id,
        });
    }
    /// Queue a drain order for the next supervisor tick.
    pub fn drain(&self, reason: &str, drain_deadline: Duration, hard_deadline: Duration) {
        self.push(ControlCommand::Drain(DrainOrder {
            reason: reason.to_owned(),
            drain_deadline,
            hard_deadline,
            traceparent: None,
        }));
    }
    /// Queue a fencing notice (the operator armed a newer epoch).
    pub fn fence(&self, expected: u64, got: u64) {
        self.push(ControlCommand::Fenced {
            expected: Epoch(expected),
            got: Epoch(got),
        });
    }
    /// Register a newer pod epoch without a `Fenced` command — the daemon
    /// detects it on the periodic `current_epoch` probe.
    pub fn set_epoch(&self, epoch: u64) {
        self.epoch.store(epoch, Ordering::Relaxed);
    }
    pub fn push(&self, command: ControlCommand) {
        self.commands
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push_back(command);
    }
    /// `Drained` reports observed so far (test assertions).
    pub fn drained_reports(&self) -> Vec<DrainedReport> {
        self.drained
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    /// Phase reports observed so far.
    pub fn phases(&self) -> Vec<String> {
        self.phases
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

impl ControlPlane for LocalControl {
    fn arm(&self) -> Result<(), BotdError> {
        Ok(())
    }

    fn wait_activate(&self, cancel: &AtomicBool) -> Result<Activation, BotdError> {
        loop {
            if let Some(activation) = self
                .pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take()
            {
                self.epoch.store(activation.epoch.get(), Ordering::Relaxed);
                return Ok(activation);
            }
            if cancel.load(Ordering::Relaxed) {
                return Err(BotdError::Unavailable(
                    "activation cancelled by shutdown".into(),
                ));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn poll_commands(&self) -> Vec<ControlCommand> {
        self.commands
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
            .collect()
    }

    fn current_epoch(&self) -> Option<Epoch> {
        let epoch = self.epoch.load(Ordering::Relaxed);
        (epoch > 0).then_some(Epoch(epoch))
    }

    fn report_drained(&self, report: DrainedReport) -> Result<(), BotdError> {
        self.drained
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(report);
        Ok(())
    }

    fn report_phase(&self, phase: &str, _detail: &str) {
        self.phases
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(phase.to_owned());
    }
}

/// Sleep in short slices so a cancel flag is honored promptly.
pub fn interruptible_sleep(duration: Duration, cancel: &AtomicBool) -> bool {
    let slice = Duration::from_millis(50);
    let mut slept = Duration::ZERO;
    while slept < duration {
        if cancel.load(Ordering::Relaxed) {
            return false;
        }
        std::thread::sleep(slice.min(duration - slept));
        slept += slice;
    }
    !cancel.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn activate(epoch: u64, snapshot_id: &str) -> GatewayCommand {
        GatewayCommand {
            command: Some(gateway_command::Command::Activate(
                fleet_types::proto::Activate {
                    epoch,
                    snapshot_id: snapshot_id.into(),
                },
            )),
        }
    }

    /// `Activate` is idempotent per pod lifetime (docs/03 §3.7): the first
    /// push arms the slot `wait_activate` consumes; a duplicate — gateway
    /// retry, session-flap redelivery — must not re-arm it or overwrite
    /// the snapshot choice mid-login.
    #[test]
    fn duplicate_activate_keeps_first_delivery() {
        let link = GatewayLink::new("bot", 2);
        dispatch(&link, activate(2, "snap-a"));
        dispatch(&link, activate(2, "snap-b"));

        let first = link
            .inner
            .activation
            .lock()
            .unwrap()
            .take()
            .expect("activation armed");
        assert_eq!(first.epoch, Epoch(2));
        assert_eq!(first.snapshot_id.unwrap().0, "snap-a");
        assert!(
            link.inner.activation.lock().unwrap().is_none(),
            "duplicate Activate re-armed the slot"
        );
    }

    /// `serving` still tracks the newest activated epoch even when the
    /// `Activation` body is deduped — a duplicate carries no state the
    /// first delivery lacked.
    #[test]
    fn activate_dedup_still_updates_serving() {
        let link = GatewayLink::new("bot", 2);
        dispatch(&link, activate(2, "snap-a"));
        dispatch(&link, activate(3, "snap-b"));
        assert_eq!(link.inner.serving.load(Ordering::Relaxed), 3);
    }
}
