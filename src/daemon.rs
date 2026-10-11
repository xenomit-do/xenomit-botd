//! The daemon: startup state machine, supervisor loop, drain.
//!
//! ```text
//! Boot → LoadConfig → Arm → Activate → Login → Attach → Run → (Drain)
//! ```
//!
//! `run()` executes the whole lifecycle on the calling thread. The
//! supervisor ticks once per `deps.tick` (1 s in production): heartbeats,
//! drains host actions and panel commands, reconciles hangar balances,
//! ticks stopped-state rules, pushes panel hello/status/config and
//! re-attaches a dropped game client unless a `Disconnect` hold is set.
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use fleet_types::Epoch;
use fleet_types::snapshot::{SnapshotId, SnapshotQuality, SnapshotStore, StoreError};
use serde_json::Value;
use xenomit_backpage::{Session, SidStatus};
use xenomit_behavior_engine::HostAction;
use xenomit_bot_runtime::handoff::{CheckpointQuality, Drain, Drained, HandoffLink};
use xenomit_bot_runtime::panel::{self, PanelLink};
use xenomit_bot_runtime::statistics::{self, Statistics};
use xenomit_bot_runtime::{
    BotSettings, FileStatSink, HostActionHandler, PlannerRuntime, SettingsStore, SharedStats,
    StatSink, set_backpage_session,
};
use xenomit_core::logging::tracing::{error, info, info_span, warn};
use xenomit_game_api::{BehaviorRuntime, GameClient, ModuleDescriptor};

use crate::config::{AuthMode, BotdConfig, RestorePolicy as ConfigRestorePolicy};
use crate::control::{
    self, ControlCommand, ControlPlane, DrainOrder, DrainedReport, GatewayControl, GatewayStore,
    StandaloneControl,
};
use crate::error::{BotdError, ExitCode, classify_attach_error};
use crate::health::{self, HealthState};
use crate::heartbeat::{Heartbeat, HeartbeatClient};
use crate::host::HeadlessHostActions;
use crate::panel as botd_panel;
use crate::proxy;
use crate::secrets::{self, SecretError, SecretResolver};
use crate::session::HeadlessSessionProvider;
use crate::snapshot::{self, LocalSnapshotStore};
use crate::state::Phase;
use crate::stats::MetricsSink;
use crate::stream::StreamPublisher;
use crate::telemetry::BotMetrics;

/// Session authentication — `HeadlessSessionProvider` in production, fakes
/// in tests.
pub trait Authenticator: Send + Sync {
    /// Validate credential availability during `Arm`.
    fn arm(&self) -> Result<(), SecretError>;
    /// Produce an authenticated Backpage session.
    fn authenticate(&self) -> Result<Session, BotdError>;
    /// Proxy exit IP observed during the last authentication, if any.
    fn exit_ip(&self) -> Option<String> {
        None
    }
}

impl Authenticator for HeadlessSessionProvider {
    fn arm(&self) -> Result<(), SecretError> {
        HeadlessSessionProvider::arm(self)
    }
    fn authenticate(&self) -> Result<Session, BotdError> {
        HeadlessSessionProvider::authenticate(self)
    }
    fn exit_ip(&self) -> Option<String> {
        HeadlessSessionProvider::exit_ip(self)
    }
}

/// Game-client attach — `PacketConnector` in production, fakes in tests.
pub trait GameConnector: Send + Sync {
    fn connect(
        &self,
        session: &Session,
        statistics: SharedStats,
        game_endpoint: Option<&str>,
    ) -> Result<Arc<dyn GameClient>, BotdError>;
}

/// Production connector: `packet_client::connect_with` on a short-lived
/// current-thread runtime. A configured `network.proxy` routes the game
/// socket through it — never direct (docs/05 §5.4, fail closed).
pub struct PacketConnector {
    network: crate::config::NetworkConfig,
    resolver: Arc<dyn SecretResolver>,
}

impl PacketConnector {
    pub fn new(network: crate::config::NetworkConfig, resolver: Arc<dyn SecretResolver>) -> Self {
        Self { network, resolver }
    }
}

impl GameConnector for PacketConnector {
    fn connect(
        &self,
        session: &Session,
        statistics: SharedStats,
        game_endpoint: Option<&str>,
    ) -> Result<Arc<dyn GameClient>, BotdError> {
        let proxy = match &self.network.proxy {
            Some(reference) => {
                let raw = self.resolver.resolve(reference).map_err(secret_error)?;
                let spec = proxy::parse_proxy(&raw).map_err(secret_error)?;
                Some(spec.to_packet_config())
            }
            None => None,
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| {
                BotdError::Fatal(format!("could not create attach runtime: {error}"))
            })?;
        let session = session.clone();
        runtime
            .block_on(xenomit_bot_runtime::packet_client::connect_via(
                session,
                statistics,
                game_endpoint,
                proxy.as_ref(),
            ))
            .map_err(|error| classify_attach_error(&error))
    }
}

/// Engine construction — the daemon supplies the host-action handler and
/// the shared drain/checkpoint channel. Production uses
/// `PlannerRuntime::builtin_only()` (no user manifests, `docs/09-security.md`
/// §9.3).
pub type EngineFactory = Arc<
    dyn Fn(Arc<HeadlessHostActions>, Arc<HandoffLink>) -> Arc<dyn BehaviorRuntime> + Send + Sync,
>;

/// Dependency bundle — real or fake.
pub struct Deps {
    pub resolver: Arc<dyn SecretResolver>,
    pub authenticator: Arc<dyn Authenticator>,
    pub connector: Arc<dyn GameConnector>,
    /// Activation grant; `StandaloneControl` without a `control` block.
    pub control: Arc<dyn ControlPlane>,
    /// Epoch-conditional snapshot store; `LocalSnapshotStore` over
    /// `state_dir` in production (docs/03 §3.8).
    pub store: Arc<dyn SnapshotStore>,
    pub engine_factory: EngineFactory,
    /// Supervisor tick — 1 s in production, ~20 ms in tests.
    pub tick: Duration,
    /// How long `Arm` retries transient resolver errors before giving up.
    pub arm_retry: Duration,
    /// Delay between re-attach attempts after a dropped session.
    pub reconnect_delay: Duration,
    /// Interval between `check_sid` probes of the live Backpage session.
    /// A dead dosid triggers an in-process re-login (password auth) or a
    /// `needs_human` hold (session auth) — `docs/02-headless-bot.md` §2.4.
    pub sid_check: Duration,
    /// Health bind; `None` disables the HTTP server (tests).
    pub health_bind: Option<SocketAddr>,
    /// Tokio runtime for the health server and signal watchers.
    pub health_rt: Option<tokio::runtime::Runtime>,
    /// Install OS signal handlers (production only).
    pub handle_signals: bool,
    /// How long the drain waits for the stat outbox to be acknowledged
    /// before exiting anyway (docs/03 §3.6 — events are journaled, so an
    /// early exit replays them instead of losing them).
    pub outbox_flush_budget: Duration,
}

/// An in-progress drain: the `DrainOrder` that started it, for the
/// `Drained` report and the degraded-snapshot reason.
struct DrainProgress {
    order: DrainOrder,
    posted_at: Instant,
}

/// Process-level termination requests, shared with signal watchers.
#[derive(Default)]
pub struct Signals {
    shutdown: AtomicBool,
    abort: AtomicBool,
}

impl Signals {
    pub fn request_shutdown(&self) {
        if self.shutdown.swap(true, Ordering::Relaxed) {
            // A second request escalates to hard abort (docs/03 §SIGTERM).
            self.abort.store(true, Ordering::Relaxed);
        }
    }
    pub fn request_abort(&self) {
        self.abort.store(true, Ordering::Relaxed);
    }
    pub fn shutdown_requested(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }
    pub fn abort_requested(&self) -> bool {
        self.abort.load(Ordering::Relaxed)
    }
}

/// Why the supervisor left the run loop.
#[derive(Debug)]
enum DrainReason {
    /// Pre-activation shutdown or a drain path with nothing running.
    Clean(&'static str),
    /// `Arm → Drain → Snapshot → Logout → exit 0` (docs/03 §3.2): the
    /// worker's drain answer (or `None` — worker gone / deadline blown)
    /// plus the trigger reason that goes into the snapshot.
    Handoff {
        drained: Option<Box<Drained>>,
        reason: String,
    },
    /// SIGQUIT or repeated SIGTERM — abort immediately.
    Abort,
    /// Failure carrying an exit code.
    Error(BotdError),
}

struct State {
    settings: BotSettings,
    session: Option<Session>,
    client: Option<Arc<dyn GameClient>>,
    statistics: Option<SharedStats>,
    modules: Vec<ModuleDescriptor>,
    /// Set by `HostAction::Disconnect`: do not re-login automatically.
    hold: bool,
    /// Next earliest attach retry after a transient failure.
    next_attach_retry: Instant,
    /// The module was running when the client dropped or a refresh was
    /// requested — it must be re-started on the next successful attach:
    /// the worker holds its own client `Arc`, so a bare re-attach leaves
    /// it driving a dead session.
    resume_module: bool,
    last_balance_fetch: Instant,
    last_sid_check: Instant,
    last_stopped_tick: Instant,
}

impl State {
    fn new(settings: BotSettings) -> Self {
        Self {
            settings,
            session: None,
            client: None,
            statistics: None,
            modules: Vec::new(),
            hold: false,
            next_attach_retry: Instant::now(),
            resume_module: false,
            last_balance_fetch: Instant::now() - Duration::from_secs(60),
            last_sid_check: Instant::now(),
            last_stopped_tick: Instant::now(),
        }
    }
}

/// The daemon. Construct via [`Daemon::production`] or [`Daemon::new`] (with
/// fake [`Deps`]); drive with [`Daemon::run`].
pub struct Daemon {
    config: BotdConfig,
    deps: Deps,
    heartbeat: Arc<Heartbeat>,
    health: Arc<HealthState>,
    signals: Arc<Signals>,
    host_actions: Arc<HeadlessHostActions>,
    panel: Arc<PanelLink>,
    /// Shared drain/checkpoint channel into the behavior worker.
    handoff: Arc<HandoffLink>,
    engine: Arc<dyn BehaviorRuntime>,
    state: Mutex<State>,
    client_logs: Arc<Mutex<VecDeque<String>>>,
    /// OTLP instruments (docs/06 §6.6) — inert when no collector is set.
    telemetry: BotMetrics,
    /// `spawn_agent` is not idempotent — arm retries must not re-spawn.
    panel_spawned: AtomicBool,
    /// Best-effort latest-state publisher; network writes happen on its own
    /// task and never hold daemon state locks.
    stream: Option<StreamPublisher>,
    stream_seq: AtomicU64,
    /// This pod's fencing epoch (0 until activated).
    epoch: AtomicU64,
    /// Drain initiated by the supervisor and answered by the worker.
    drain_state: Mutex<Option<DrainProgress>>,
    /// Serialized bytes of the last committed checkpoint — coalescing:
    /// unchanged state is not rewritten (docs/03 §3.3).
    last_checkpoint_bytes: Mutex<Vec<u8>>,
}

impl Daemon {
    /// Wire production dependencies for `config`.
    pub fn production(config: BotdConfig) -> Arc<Self> {
        let resolver = secrets::resolver_for(&config);
        let authenticator: Arc<dyn Authenticator> = Arc::new(HeadlessSessionProvider::new(
            config.account.clone(),
            config.testing.clone(),
            resolver.clone(),
            config.network.clone(),
            config.exit_ip_url(),
        ));
        let engine_factory: EngineFactory = Arc::new(|handler, handoff| {
            Arc::new(
                PlannerRuntime::builtin_only()
                    .with_host_action_handler(handler)
                    .with_handoff_link(handoff),
            ) as Arc<dyn BehaviorRuntime>
        });
        let health_bind = config.health.bind.parse::<SocketAddr>().ok().or_else(|| {
            // host:port with a DNS name — resolve lazily at bind time.
            use std::net::ToSocketAddrs;
            config.health.bind.to_socket_addrs().ok()?.next()
        });
        let health_rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .ok();
        // Managed mode: a resolvable control endpoint swaps the standalone
        // control plane for the real `FleetControl` stream, and the local
        // snapshot store for one that uploads through the gateway (with a
        // local mirror for `get`/fencing). No endpoint → standalone.
        let local = LocalSnapshotStore::new(config.state_dir.clone());
        let (control, store): (Arc<dyn ControlPlane>, Arc<dyn SnapshotStore>) =
            match (config.control.as_ref(), config.control_endpoint()) {
                (Some(control), Some(endpoint)) => {
                    let gateway = GatewayControl::new(
                        endpoint,
                        control.token.clone(),
                        resolver.clone(),
                        config.bot.id.clone(),
                        config.pod_epoch(),
                    );
                    let store = GatewayStore::new(gateway.link(), config.state_dir.clone());
                    (Arc::new(gateway), Arc::new(store))
                }
                _ => (Arc::new(StandaloneControl::new()), Arc::new(local)),
            };
        let connector: Arc<dyn GameConnector> = if config.client.kind == "flash" {
            Arc::new(crate::flash::FlashConnector)
        } else {
            Arc::new(PacketConnector::new(
                config.network.clone(),
                resolver.clone(),
            ))
        };
        Self::new(
            config,
            Deps {
                resolver,
                authenticator,
                connector,
                control,
                store,
                engine_factory,
                tick: Duration::from_secs(1),
                arm_retry: Duration::from_secs(60),
                reconnect_delay: Duration::from_secs(5),
                sid_check: Duration::from_secs(30),
                health_bind,
                health_rt,
                handle_signals: true,
                outbox_flush_budget: Duration::from_secs(10),
            },
        )
    }

    /// Custom dependencies (tests inject fakes).
    pub fn new(config: BotdConfig, deps: Deps) -> Arc<Self> {
        let heartbeat = Arc::new(Heartbeat::new());
        let host_actions = Arc::new(HeadlessHostActions::default());
        // Settings flow through the read-only SettingsStore seam.
        let settings =
            crate::settings::ConfigSettingsStore::new(config.bot_settings().unwrap_or_default())
                .load();
        let telemetry = BotMetrics::new();
        let checkpoint_interval = (config.handoff.checkpoint_interval_s > 0)
            .then(|| Duration::from_secs(config.handoff.checkpoint_interval_s));
        let handoff = Arc::new(HandoffLink::new(checkpoint_interval));
        let stream = deps
            .health_rt
            .as_ref()
            .and_then(|runtime| StreamPublisher::spawn(runtime.handle()));
        Arc::new(Self {
            engine: (deps.engine_factory)(host_actions.clone(), handoff.clone()),
            state: Mutex::new(State::new(settings)),
            config,
            deps,
            health: Arc::new(HealthState::new(heartbeat.clone())),
            heartbeat,
            signals: Arc::new(Signals::default()),
            host_actions,
            panel: Arc::new(PanelLink::default()),
            handoff,
            telemetry,
            client_logs: Arc::new(Mutex::new(VecDeque::new())),
            panel_spawned: AtomicBool::new(false),
            stream,
            stream_seq: AtomicU64::new(0),
            epoch: AtomicU64::new(0),
            drain_state: Mutex::new(None),
            last_checkpoint_bytes: Mutex::new(Vec::new()),
        })
    }

    pub fn signals(&self) -> Arc<Signals> {
        self.signals.clone()
    }
    pub fn health_state(&self) -> Arc<HealthState> {
        self.health.clone()
    }
    pub fn heartbeat(&self) -> Arc<Heartbeat> {
        self.heartbeat.clone()
    }
    pub fn host_actions(&self) -> Arc<HeadlessHostActions> {
        self.host_actions.clone()
    }
    /// The session's event-sourced statistics (tests inspect the outbox).
    pub fn statistics(&self) -> Option<SharedStats> {
        self.state.lock().ok().and_then(|s| s.statistics.clone())
    }
    /// The epoch-conditional snapshot store (tests read/write it directly).
    pub fn store(&self) -> Arc<dyn SnapshotStore> {
        self.deps.store.clone()
    }
    /// The shared drain/checkpoint channel (tests post drains/restores).
    pub fn handoff(&self) -> Arc<HandoffLink> {
        self.handoff.clone()
    }
    /// This pod's fencing epoch — 0 before Activate.
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Relaxed)
    }
    pub fn phase(&self) -> Phase {
        self.health.phase()
    }
    pub fn engine(&self) -> Arc<dyn BehaviorRuntime> {
        self.engine.clone()
    }
    /// Current attached session (tests/inspection).
    pub fn session(&self) -> Option<Session> {
        self.state.lock().ok().and_then(|s| s.session.clone())
    }

    fn set_phase(&self, phase: Phase) {
        info!(%phase, "phase");
        self.telemetry.set_phase(phase);
        self.health.set_phase(phase);
        self.deps.control.report_phase(&phase.to_string(), "");
    }

    /// Full lifecycle → process exit code.
    pub fn run(self: &Arc<Self>) -> ExitCode {
        self.start_sidecars();
        match self.lifecycle() {
            Ok(code) => code,
            Err(error) => {
                error!(%error, "daemon failed");
                self.health.set_last_error(Some(error.to_string()));
                self.health
                    .set_needs_human(error.needs_human().map(str::to_owned));
                self.drain(DrainReason::Error(error))
            }
        }
    }

    fn start_sidecars(self: &Arc<Self>) {
        if let Some(runtime) = &self.deps.health_rt {
            if let Some(bind) = self.deps.health_bind {
                match health::spawn(runtime.handle(), bind, self.health.clone()) {
                    Ok(bound) => info!(%bound, "health endpoints listening"),
                    Err(error) => warn!(%error, "health server failed to bind"),
                }
            }
            if self.deps.handle_signals {
                install_signal_handlers(runtime, self.signals.clone());
            }
        } else if self.deps.handle_signals {
            install_signal_handlers_std(self.signals.clone());
        }
    }

    fn lifecycle(self: &Arc<Self>) -> Result<ExitCode, BotdError> {
        self.telemetry.record_start();
        self.set_phase(Phase::Arm);
        self.arm()?;
        self.health.mark_started();
        if self.signals.shutdown_requested() {
            return Ok(self.drain(DrainReason::Clean("shutdown requested")));
        }
        self.set_phase(Phase::AwaitActivate);
        let activation = self.deps.control.wait_activate(&self.signals.shutdown)?;
        self.epoch.store(activation.epoch.get(), Ordering::Relaxed);
        // The epoch write is the fence: a second `botd` on the same bot_id
        // now monotones it, and every epoch-conditional `put` after this
        // point rejects stale writers (docs/03 §3.7).
        snapshot::block_on(
            self.deps
                .store
                .record_epoch(&self.config.bot.id, activation.epoch),
        )
        .map_err(|error| BotdError::Fatal(format!("snapshot store epoch write failed: {error}")))?;
        info!(epoch = activation.epoch.get(), "activated");
        if self.signals.shutdown_requested() {
            return Ok(self.drain(DrainReason::Clean("shutdown requested")));
        }
        self.set_phase(Phase::Login);
        // docs/03 §3.4: `GetSnapshot` runs before login — a strict pod with
        // an unreadable snapshot exits 70 without ever touching the game.
        let fetched = match &activation.snapshot_id {
            Some(id) => self.fetch_snapshot(id)?,
            None => None,
        };
        // Restore goes through attach so the outbox tail replays before the
        // session's first journal append — seq numbering must continue past
        // the restored events or the streams would collide.
        let posted_quality = self.attach(fetched)?;
        self.set_phase(Phase::Run);
        self.health.set_ready(true);
        if self.config.autostart {
            self.start_module()?;
        }
        self.await_restore_result(posted_quality)?;
        let reason = self.supervise();
        Ok(self.drain(reason))
    }

    // ------------------------------------------------------------------
    // Arm
    // ------------------------------------------------------------------

    fn arm(&self) -> Result<(), BotdError> {
        let deadline = Instant::now() + self.deps.arm_retry;
        loop {
            match self.try_arm() {
                Ok(()) => return Ok(()),
                Err(error @ BotdError::Unavailable(_)) => {
                    if self.signals.shutdown_requested() || Instant::now() >= deadline {
                        return Err(error);
                    }
                    warn!(%error, "arm not ready — retrying");
                    if !control::interruptible_sleep(
                        Duration::from_secs(5).min(self.deps.arm_retry),
                        &self.signals.shutdown,
                    ) {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn try_arm(&self) -> Result<(), BotdError> {
        // Credentials — the most likely transient (projected volumes appear late).
        self.deps.authenticator.arm().map_err(secret_error)?;
        // Egress proxy — verify reachability; routing is a later phase.
        if let Some(reference) = &self.config.network.proxy {
            let raw = self
                .deps
                .resolver
                .resolve(reference)
                .map_err(secret_error)?;
            let spec = proxy::parse_proxy(&raw).map_err(secret_error)?;
            proxy::check_egress(&spec, Duration::from_secs(10)).map_err(BotdError::ProxyFailure)?;
            info!(scheme = %spec.scheme, host = %spec.host, port = spec.port, "proxy verified");
        } else if self.config.network.require_proxy {
            return Err(BotdError::ProxyFailure(
                "network.require_proxy is set but network.proxy is not configured".into(),
            ));
        }
        // Panel pairing token → configure + spawn the agent (spawn once).
        if let Some(panel) = &self.config.panel {
            let raw = self
                .deps
                .resolver
                .resolve(&panel.instance_token)
                .map_err(secret_error)?;
            let token = secrets::parse_token(&raw, "panel instance token").map_err(secret_error)?;
            botd_panel::configure_link(&self.panel, &self.config, &token);
            if !self.panel_spawned.swap(true, Ordering::Relaxed) {
                panel::spawn_agent(self.panel.clone());
            }
        }
        // Control channel — `GatewayControl` resolves its token through
        // the resolver and spawns the FleetControl stream; standalone is
        // a no-op.
        self.deps.control.arm()?;
        Ok(())
    }

    // ------------------------------------------------------------------
    // Login + Attach
    // ------------------------------------------------------------------

    /// `login → attach`. A fetched [`snapshot::RestorePlan`] is applied to
    /// the fresh journal before the first event is appended so restored
    /// `seq`s continue monotonically. Returns the posted checkpoint
    /// quality for [`Self::await_restore_result`].
    fn attach(
        &self,
        restore: Option<snapshot::RestorePlan>,
    ) -> Result<Option<CheckpointQuality>, BotdError> {
        self.set_phase(Phase::Login);
        let login_started = Instant::now();
        let login = info_span!("bot.login").in_scope(|| self.deps.authenticator.authenticate());
        self.telemetry.record_login(
            login_started.elapsed(),
            if login.is_ok() { "ok" } else { "failed" },
        );
        let session = login?;
        set_backpage_session(Some(session.clone()));
        // Egress identity for the control plane/status (docs/05 §5.4).
        if let Some(ip) = self.deps.authenticator.exit_ip() {
            self.deps.control.report_exit_ip(&ip);
        }

        let metrics = Arc::new(MetricsSink::new(self.telemetry.clone())) as Arc<dyn StatSink>;
        let directory = self.config.state_dir.join("statistics");
        let statistics = Statistics::load_with_sinks(&session, directory, move |path| {
            vec![
                Arc::new(FileStatSink::new(path)) as Arc<dyn StatSink>,
                metrics,
            ]
        });
        let mut posted_quality = None;
        if let Ok(mut stats) = statistics.lock() {
            stats.panel = Some(self.panel.clone());
            if let Some(plan) = restore {
                stats.restore_outbox(plan.outbox_cursor, plan.outbox_tail);
                posted_quality = Some(plan.pending.quality);
                self.handoff.post_restore(plan.pending);
            }
        }
        self.panel.set_logs(self.client_logs.clone());

        self.set_phase(Phase::Attach);
        let endpoint = self
            .config
            .testing
            .as_ref()
            .and_then(|t| t.game_endpoint.clone());
        let client = info_span!("bot.attach").in_scope(|| {
            self.deps
                .connector
                .connect(&session, statistics.clone(), endpoint.as_deref())
        })?;
        let client: Arc<dyn GameClient> =
            Arc::new(HeartbeatClient::new(client, self.heartbeat.clone()));

        let modules = self.catalog()?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| BotdError::Fatal("state lock poisoned".into()))?;
        self.log_stat_locked(&statistics, "session attached");
        state.session = Some(session);
        state.statistics = Some(statistics);
        state.client = Some(client);
        state.modules = modules;
        state.hold = false;
        // Fresh sid: restart the expiry probe window.
        state.last_sid_check = Instant::now();
        drop(state);
        self.health.set_needs_human(None);
        self.health.set_last_error(None);
        self.health.set_ready(true);
        Ok(posted_quality)
    }

    // ------------------------------------------------------------------
    // Restore (docs/03 §3.4–3.5)
    // ------------------------------------------------------------------

    /// `GetSnapshot` (docs/03 §3.4): fetch and decode the activation
    /// snapshot *before* login so a strict failure never touches the game
    /// session. Returns the plan for [`Self::post_restore`], or `None` when
    /// nothing restores (missing snapshot, or corrupt under best-effort).
    fn fetch_snapshot(
        &self,
        snapshot_id: &SnapshotId,
    ) -> Result<Option<snapshot::RestorePlan>, BotdError> {
        let strict = self.config.handoff.restore_policy == ConfigRestorePolicy::Strict;
        let span = info_span!("snapshot.fetch", %snapshot_id, strict);
        let stored = span.in_scope(|| snapshot::block_on(self.deps.store.get(snapshot_id)));
        let stored = match stored {
            Ok(stored) => stored,
            Err(StoreError::NotFound(_)) => {
                // A lost snapshot is not corruption — the successor starts
                // fresh in both policies (docs/03 §3.5).
                warn!(%snapshot_id, "activation snapshot missing — fresh start");
                return Ok(None);
            }
            Err(error) => {
                return self.restore_failure(strict, format!("snapshot fetch: {error}"));
            }
        };
        match snapshot::restore_plan(
            &stored,
            if strict {
                xenomit_bot_runtime::handoff::RestorePolicy::Strict
            } else {
                xenomit_bot_runtime::handoff::RestorePolicy::BestEffort
            },
        ) {
            Ok(plan) => {
                info!(%snapshot_id, reason = ?plan.reason, "snapshot fetched for restore");
                Ok(Some(plan))
            }
            Err(error) => self.restore_failure(strict, error),
        }
    }

    /// Shared best-effort/strict decision for snapshot failures: strict
    /// aborts activation (exit 70); best-effort logs and starts fresh.
    fn restore_failure<T>(&self, strict: bool, error: String) -> Result<Option<T>, BotdError> {
        if strict {
            Err(BotdError::SnapshotUnreadable(error))
        } else {
            warn!(%error, "snapshot restore skipped — fresh start");
            Ok(None)
        }
    }

    /// After `start_module`: wait briefly for the worker's restore verdict.
    /// Strict mode aborts (exit 70) only when a *clean* snapshot failed —
    /// degraded data never blocks a pod (docs/03 §3.5).
    fn await_restore_result(&self, quality: Option<CheckpointQuality>) -> Result<(), BotdError> {
        let Some(quality) = quality else {
            return Ok(());
        };
        if !self.engine.is_running() {
            // Module not started (autostart off): the pending restore applies
            // whenever it does; a strict abort only matters once it runs.
            return Ok(());
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(result) = self.handoff.take_restore_result() {
                match result {
                    Ok(()) => {
                        info!("snapshot restored");
                        return Ok(());
                    }
                    Err(error) => {
                        let strict = self.config.handoff.restore_policy
                            == ConfigRestorePolicy::Strict
                            && quality == CheckpointQuality::Clean;
                        if strict {
                            return Err(BotdError::SnapshotUnreadable(error));
                        }
                        warn!(%error, "snapshot restore failed — module started fresh");
                        return Ok(());
                    }
                }
            }
            if Instant::now() >= deadline {
                warn!("restore result never arrived — assuming applied");
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn catalog(&self) -> Result<Vec<ModuleDescriptor>, BotdError> {
        let plugins = self.engine.plugin_descriptors().map_err(BotdError::Fatal)?;
        let all_enabled: std::collections::BTreeMap<String, bool> = plugins
            .iter()
            .map(|plugin| (plugin.id.clone(), true))
            .collect();
        let modules = self
            .engine
            .modules_for(&all_enabled)
            .map_err(BotdError::Fatal)?;
        Ok(botd_panel::catalog_descriptors(modules))
    }

    fn detach(&self) {
        self.health.set_ready(false);
        self.heartbeat.set_worker_expected(false);
        if let Ok(mut state) = self.state.lock() {
            state.client = None;
            state.session = None;
            state.statistics = None;
        }
        set_backpage_session(None);
    }

    fn stop_module(&self) {
        self.heartbeat.set_worker_expected(false);
        if let Err(error) = self.engine.stop() {
            warn!(%error, "module stop failed");
        }
    }

    fn start_module(&self) -> Result<(), BotdError> {
        let (client, config) = {
            let state = self
                .state
                .lock()
                .map_err(|_| BotdError::Fatal("state lock poisoned".into()))?;
            let client = state
                .client
                .clone()
                .ok_or_else(|| BotdError::Unavailable("no game client attached".into()))?;
            (client, state.settings.module_config())
        };
        self.heartbeat.set_worker_expected(false);
        self.engine
            .start(client, config)
            .map_err(|error| BotdError::Fatal(format!("module start failed: {error}")))?;
        self.heartbeat.set_worker_expected(true);
        if let Ok(mut state) = self.state.lock() {
            state.resume_module = false;
        }
        self.log_stat("module started");
        Ok(())
    }

    /// Re-login in-process on `HostAction::Refresh`/`RefreshIdle`
    /// (`docs/03-graceful-handoff.md`: stop → re-login → reload → resume).
    fn refresh(&self, resume: bool) -> Result<(), BotdError> {
        info!(resume, "refresh: re-login in-process");
        self.stop_module();
        if let Ok(mut state) = self.state.lock() {
            // If this attach fails, `check_client` retries and the flag
            // survives so a later success still brings the module back.
            state.resume_module = resume;
        }
        self.detach();
        self.set_phase(Phase::Login);
        // Re-login after attach is never a snapshot restore — refreshes are
        // in-process (docs/03 §3.4: restore only happens at Activate).
        self.attach(None)?;
        self.set_phase(Phase::Run);
        if resume {
            self.start_module()?;
        }
        Ok(())
    }

    /// Mid-run refresh failure (§2.4): operator-action errors park on
    /// `needs_human`; transient ones behave like a dropped session —
    /// detached, retried by `check_client`, module resumed on success.
    /// Neither path exits the process.
    fn refresh_failed(&self, error: &BotdError) {
        warn!(%error, "refresh failed");
        self.health.set_last_error(Some(error.to_string()));
        if let Some(kind) = error.needs_human() {
            self.health.set_needs_human(Some(kind.to_owned()));
            if let Ok(mut state) = self.state.lock() {
                state.hold = true;
            }
        } else if let Ok(mut state) = self.state.lock() {
            state.next_attach_retry = Instant::now() + self.deps.reconnect_delay;
        }
    }

    /// After a successful `check_client` re-attach: restart the module on
    /// the new client if it was running (or a refresh wanted it running)
    /// before the drop — the old worker is bound to the dead session.
    fn resume_module_after_attach(&self) {
        let resume = self
            .state
            .lock()
            .map(|mut state| std::mem::take(&mut state.resume_module))
            .unwrap_or(false);
        if !resume {
            return;
        }
        self.stop_module();
        if let Err(error) = self.start_module() {
            warn!(%error, "module resume failed after re-attach");
        }
    }

    // ------------------------------------------------------------------
    // Supervisor
    // ------------------------------------------------------------------

    fn supervise(self: &Arc<Self>) -> DrainReason {
        loop {
            let tick_started = Instant::now();
            self.heartbeat.beat_supervisor();
            if self.signals.abort_requested() {
                return DrainReason::Abort;
            }
            // SIGTERM is just another drain trigger (docs/03 §3.2 — every
            // trigger runs the same path; `eviction` for process signals).
            if self.signals.shutdown_requested() {
                self.begin_handoff(self.configured_drain("eviction"));
            }
            if let Some(reason) = self.handle_host_actions() {
                return reason;
            }
            self.dispatch_panel_commands();
            if let Some(reason) = self.poll_control() {
                return reason;
            }
            if let Some(error) = self.probe_fence() {
                return DrainReason::Error(error);
            }
            if let Some(reason) = self.flush_checkpoints() {
                return reason;
            }
            self.drain_stat_acks();
            // While draining the worker owns the client; the supervisor
            // stops re-attaching/probing and just waits for `Drained`.
            if !self.draining() {
                self.check_client();
                self.backpage_probe();
                self.tick_stopped();
            }
            self.push_panel_state();
            self.telemetry
                .record_tick(&self.active_module_label(), tick_started.elapsed());
            if let Some(reason) = self.check_drained() {
                return reason;
            }
            if !control::interruptible_sleep(self.deps.tick, &self.signals.shutdown)
                && self.signals.abort_requested()
            {
                return DrainReason::Abort;
            }
        }
    }

    // ------------------------------------------------------------------
    // Drain orchestration (docs/03 §3.2)
    // ------------------------------------------------------------------

    /// A `DrainOrder` for a local trigger (SIGTERM, panel shutdown, host
    /// Close) — deadlines come from `handoff.*` config.
    fn configured_drain(&self, reason: &str) -> DrainOrder {
        DrainOrder {
            reason: reason.to_owned(),
            drain_deadline: Duration::from_secs(self.config.handoff.drain_deadline_s),
            hard_deadline: Duration::from_secs(self.config.handoff.hard_deadline_s),
            traceparent: None,
        }
    }

    fn draining(&self) -> bool {
        self.drain_state
            .lock()
            .map(|state| state.is_some())
            .unwrap_or(false)
    }

    /// Start (or merge into) a drain: post the directive to the worker and
    /// record the progress the supervisor waits on. Idempotent — drains are
    /// not re-entrant.
    fn begin_handoff(&self, order: DrainOrder) {
        {
            let Ok(mut slot) = self.drain_state.lock() else {
                return;
            };
            if slot.is_some() {
                return;
            }
            info!(reason = %order.reason, "drain requested");
            self.set_phase(Phase::Draining);
            *slot = Some(DrainProgress {
                order: order.clone(),
                posted_at: Instant::now(),
            });
        }
        if self.engine.is_running() {
            let directive = Drain {
                reason: order.reason.clone(),
                drain_deadline: order.drain_deadline,
                hard_deadline: order.hard_deadline,
                traceparent: order.traceparent.clone(),
            };
            if let Err(directive) = self.handoff.post_drain(directive) {
                warn!(reason = %directive.reason, "drain directive already posted");
            }
        }
    }

    /// Has the drain concluded? The worker answers with [`Drained`]; when it
    /// is gone (or the hard deadline plus a grace margin passed) the daemon
    /// falls back to a stats-only degraded snapshot so it still logs out and
    /// exits 0 (docs/03 §3.2/§3.7).
    fn check_drained(&self) -> Option<DrainReason> {
        if let Some(drained) = self.handoff.take_drained() {
            let reason = self
                .drain_state
                .lock()
                .ok()
                .and_then(|mut slot| slot.take())
                .map(|progress| progress.order.reason)
                .unwrap_or_else(|| "manual_restart".to_owned());
            return Some(DrainReason::Handoff {
                drained: Some(Box::new(drained)),
                reason,
            });
        }
        let Ok(mut slot) = self.drain_state.lock() else {
            return None;
        };
        let progress = slot.as_ref()?;
        // 15 s grace past the hard deadline for the worker's post-stop write.
        let overdue =
            progress.posted_at.elapsed() > progress.order.hard_deadline + Duration::from_secs(15);
        if !self.engine.is_running() || overdue {
            let reason = slot.take().map(|p| p.order.reason).unwrap_or_default();
            return Some(DrainReason::Handoff {
                drained: None,
                reason,
            });
        }
        None
    }

    /// Apply queued gateway commands (`GatewayCommand`, fleet.proto).
    fn poll_control(&self) -> Option<DrainReason> {
        for command in self.deps.control.poll_commands() {
            match command {
                ControlCommand::Drain(order) => self.begin_handoff(order),
                ControlCommand::Shutdown { reason } | ControlCommand::Pause { reason } => {
                    // desired_state=stopped|paused drains with no successor.
                    let mut order = self.configured_drain(&reason);
                    order.reason = "desired_state".into();
                    self.begin_handoff(order);
                }
                ControlCommand::Resume { reason } => {
                    info!(%reason, "resume requested");
                    if !self.engine.is_running()
                        && let Err(error) = self.start_module()
                    {
                        warn!(%error, "resume failed");
                    }
                }
                ControlCommand::ApplyConfig { settings, .. } => {
                    match serde_json::from_value::<BotSettings>(settings) {
                        Ok(settings) => {
                            let config = {
                                let Ok(mut state) = self.state.lock() else {
                                    continue;
                                };
                                state.settings = settings;
                                state.settings.module_config()
                            };
                            self.engine.update(&config);
                        }
                        Err(error) => warn!(%error, "ApplyConfig payload invalid — ignored"),
                    }
                }
                ControlCommand::Fenced { expected, got } => {
                    return Some(DrainReason::Error(BotdError::Fenced(format!(
                        "epoch {} is stale — current epoch is {}",
                        got.get(),
                        expected.get()
                    ))));
                }
            }
        }
        None
    }

    /// Mid-tick fence probe (docs/03 §3.7): a newer epoch in the store or on
    /// the control plane means this pod is stale — exit 67 before touching
    /// the session further.
    fn probe_fence(&self) -> Option<BotdError> {
        let mine = Epoch(self.epoch.load(Ordering::Relaxed));
        if mine == Epoch::ZERO {
            return None;
        }
        let store_epoch = snapshot::block_on(self.deps.store.current_epoch(&self.config.bot.id))
            .ok()
            .flatten();
        let control_epoch = self.deps.control.current_epoch();
        for observed in [store_epoch, control_epoch].into_iter().flatten() {
            if observed > mine {
                return Some(BotdError::Fenced(format!(
                    "epoch {} is stale — observed {}",
                    mine.get(),
                    observed.get()
                )));
            }
        }
        None
    }

    /// Periodic checkpoints posted by the worker → epoch-conditional
    /// `put`. Coalesces on identical payloads; a `Fenced` answer means the
    /// store saw a newer epoch and this pod must exit 67.
    fn flush_checkpoints(&self) -> Option<DrainReason> {
        while let Some(checkpoint) = self.handoff.take_checkpoint() {
            let Ok(bytes) = serde_json::to_vec(&checkpoint) else {
                continue;
            };
            {
                let Ok(mut last) = self.last_checkpoint_bytes.lock() else {
                    return None;
                };
                if *last == bytes {
                    continue;
                }
                *last = bytes;
            }
            let snapshot = self.assemble_snapshot(
                Some(checkpoint),
                snapshot::wire_reason("checkpoint"),
                SnapshotQuality::Checkpoint,
            );
            match snapshot::block_on(self.deps.store.put(&snapshot)) {
                Ok(_) => {}
                Err(StoreError::Fenced(fenced)) => {
                    return Some(DrainReason::Error(BotdError::Fenced(format!(
                        "checkpoint write fenced: {fenced}"
                    ))));
                }
                Err(error) => warn!(%error, "checkpoint write failed"),
            }
        }
        None
    }

    /// Build the wire `BotSnapshot` for the current session.
    fn assemble_snapshot(
        &self,
        checkpoint: Option<xenomit_bot_runtime::handoff::RuntimeCheckpoint>,
        reason: fleet_types::snapshot::SnapshotReason,
        quality: SnapshotQuality,
    ) -> fleet_types::snapshot::BotSnapshot {
        let (statistics, active_module, run_state) = {
            let Ok(state) = self.state.lock() else {
                return snapshot::assemble(
                    &self.config.bot.id,
                    Epoch(self.epoch.load(Ordering::Relaxed)),
                    reason,
                    quality,
                    checkpoint,
                    None,
                    0,
                    "unknown",
                    "",
                );
            };
            (
                state.statistics.clone(),
                state.settings.active_module.clone(),
                if self.engine.is_running() {
                    "running"
                } else {
                    "stopped"
                },
            )
        };
        snapshot::assemble(
            &self.config.bot.id,
            Epoch(self.epoch.load(Ordering::Relaxed)),
            reason,
            quality,
            checkpoint,
            statistics.as_ref().and_then(|s| s.lock().ok()).as_deref(),
            0,
            run_state,
            &active_module,
        )
    }

    /// `stat_ack` cursors from the panel → `Statistics::ack` (docs/03 §3.6).
    fn drain_stat_acks(&self) {
        let cursors = self.panel.take_stat_acks();
        if cursors.is_empty() {
            return;
        }
        if let Some(statistics) = self.statistics()
            && let Ok(mut stats) = statistics.lock()
        {
            for cursor in cursors {
                stats.ack(cursor);
            }
        }
    }

    /// Bounded `module` label for `xenomit.runtime.tick_duration`.
    fn active_module_label(&self) -> String {
        if !self.engine.is_running() {
            return "idle".into();
        }
        self.state
            .lock()
            .map(|state| state.settings.active_module.clone())
            .ok()
            .filter(|module| !module.is_empty())
            .unwrap_or_else(|| "unknown".into())
    }

    /// Bridge liveness: a dead packet session drops to detached and re-logins
    /// after `reconnect_delay` unless a `Disconnect` hold is set.
    fn check_client(&self) {
        let client = {
            let state = match self.state.lock() {
                Ok(state) => state,
                Err(_) => return,
            };
            match state.client.clone() {
                Some(client) => client,
                None => {
                    if !state.hold && Instant::now() >= state.next_attach_retry {
                        drop(state);
                        match self.attach(None) {
                            Ok(_) => {
                                self.set_phase(Phase::Run);
                                self.resume_module_after_attach();
                            }
                            Err(error) if error.needs_human().is_some() => {
                                self.health
                                    .set_needs_human(error.needs_human().map(str::to_owned));
                                self.health.set_last_error(Some(error.to_string()));
                                if let Ok(mut state) = self.state.lock() {
                                    state.hold = true;
                                }
                                warn!(%error, "attach needs human action — holding");
                            }
                            Err(error) => {
                                self.health.set_last_error(Some(error.to_string()));
                                warn!(%error, "re-attach failed — retrying");
                                if let Ok(mut state) = self.state.lock() {
                                    state.next_attach_retry =
                                        Instant::now() + self.deps.reconnect_delay;
                                }
                            }
                        }
                    }
                    return;
                }
            }
        };
        // `status()` is blocking I/O — never run it under the state lock.
        match client.status() {
            Ok(status) if status.ready => {}
            Ok(_) | Err(_) => {
                warn!("game client lost readiness — detaching");
                self.log_stat("game client disconnected");
                let was_running = self.engine.is_running();
                self.detach();
                if let Ok(mut state) = self.state.lock() {
                    // The worker still holds the dead client; it must be
                    // re-started on the fresh session.
                    state.resume_module = was_running;
                    state.next_attach_retry = Instant::now() + self.deps.reconnect_delay;
                }
            }
        }
    }

    fn handle_host_actions(self: &Arc<Self>) -> Option<DrainReason> {
        for action in self.host_actions.drain() {
            match action {
                HostAction::Refresh => {
                    let resume = self.engine.is_running();
                    if let Err(error) = self.refresh(resume) {
                        self.refresh_failed(&error);
                    }
                }
                HostAction::RefreshIdle => {
                    if let Err(error) = self.refresh(false) {
                        self.refresh_failed(&error);
                    }
                }
                // Cooperative refresh stays an internal worker signal.
                HostAction::RequestRefresh => {}
                HostAction::Disconnect => {
                    warn!("host action: disconnect — dropping client, holding re-login");
                    self.log_stat("host action: disconnect");
                    self.stop_module();
                    self.detach();
                    if let Ok(mut state) = self.state.lock() {
                        state.hold = true;
                        state.resume_module = false;
                    }
                }
                HostAction::Close => {
                    // A quit request runs the same drain path as SIGTERM —
                    // `manual_restart` reason (docs/03 §3.2).
                    self.begin_handoff(self.configured_drain("manual_restart"));
                }
                HostAction::Profile(name) => {
                    warn!(%name, "profile switch unsupported headless — settings are read-only");
                }
                HostAction::TagPlayer { id, tag } => {
                    warn!(%id, %tag, "player tags are read-only in botd — ignored");
                }
                HostAction::SelectTarget(_) => {} // runtime-internal
                HostAction::AcceptUrgentQuest | HostAction::ClaimCalendar => {
                    warn!("host action needs a game service unavailable headless — ignored");
                }
            }
        }
        None
    }

    fn dispatch_panel_commands(self: &Arc<Self>) {
        for command in self.panel.take_commands() {
            let daemon = self.clone();
            let _ = std::thread::Builder::new()
                .name("panel-command".into())
                .spawn(move || daemon.handle_command(command));
        }
    }

    fn handle_command(self: &Arc<Self>, command: panel::PanelCommand) {
        let id = command.id.clone();
        let reply = |ok: bool, error: Option<String>, data: Option<Value>| {
            self.panel.reply(&id, ok, error, data);
        };
        match command.action.as_str() {
            "start" => {
                if self.engine.is_running() {
                    reply(false, Some("Module already running".into()), None);
                } else {
                    match self.start_module() {
                        Ok(()) => reply(true, None, None),
                        Err(error) => reply(false, Some(error.to_string()), None),
                    }
                }
            }
            "stop" => {
                self.stop_module();
                reply(true, None, None);
            }
            "set_module" => {
                let module_id = command.payload["module_id"].as_str().unwrap_or_default();
                let known = self
                    .state
                    .lock()
                    .map(|s| s.modules.iter().any(|d| d.id == module_id))
                    .unwrap_or(false);
                if module_id.is_empty() || !known {
                    reply(false, Some(format!("Unknown module {module_id}")), None);
                } else if self.engine.is_running() {
                    reply(
                        false,
                        Some("Cannot switch module while running".into()),
                        None,
                    );
                } else if let Ok(mut state) = self.state.lock() {
                    state.settings.active_module = module_id.to_owned();
                    reply(true, None, None);
                }
            }
            "update_config" => {
                let module_id = command.payload["module_id"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned();
                match botd_panel::unflatten(&command.payload["settings"])
                    .and_then(|nested| self.apply_config_update(&module_id, nested))
                {
                    Ok(()) => reply(true, None, None),
                    Err(error) => reply(false, Some(error), None),
                }
            }
            "game_command" => {
                let client = self
                    .state
                    .lock()
                    .ok()
                    .and_then(|state| state.client.clone());
                match (client, panel::parse_game_command(&command.payload)) {
                    (Some(client), Ok(game_command)) => match client.game_command(&game_command) {
                        Ok(result) => reply(true, None, result.data),
                        Err(error) => reply(false, Some(error.to_string()), None),
                    },
                    (None, _) => reply(false, Some("No game client attached".into()), None),
                    (_, Err(error)) => reply(false, Some(error), None),
                }
            }
            "refresh_stats" => {
                let session = self.session();
                match session {
                    None => reply(false, Some("No Backpage session".into()), None),
                    Some(session) => match statistics::fetch_balances(session) {
                        Ok(balances) => {
                            let stats = self.state.lock().ok().and_then(|s| s.statistics.clone());
                            match stats.and_then(|s| {
                                s.lock().ok().map(|mut s| {
                                    s.reconcile(balances, "panel", statistics::now_ms());
                                })
                            }) {
                                Some(()) => reply(true, None, Some(serde_json::json!(balances))),
                                None => reply(false, Some("Statistics unavailable".into()), None),
                            }
                        }
                        Err(error) => reply(false, Some(error), None),
                    },
                }
            }
            "reset_stats" => {
                let stats = self.state.lock().ok().and_then(|s| s.statistics.clone());
                let cleared = stats.is_some_and(|stats| {
                    stats.lock().is_ok_and(|mut stats| {
                        stats.events.clear();
                        stats.balances = [None; 4];
                        stats.started = statistics::now_ms();
                        stats.ended = None;
                        true
                    })
                });
                if cleared {
                    reply(true, None, None);
                } else {
                    reply(false, Some("No statistics for this session".into()), None);
                }
            }
            "shutdown" => {
                reply(true, None, None);
                self.signals.request_shutdown();
            }
            other => reply(false, Some(format!("Unknown action {other}")), None),
        }
    }

    /// Apply a panel `update_config` to the in-memory (read-only) settings
    /// and push the effective module config to a running worker.
    fn apply_config_update(&self, module_id: &str, nested: Value) -> Result<(), String> {
        let mut state = self.state.lock().map_err(|_| "state lock poisoned")?;
        match module_id {
            "__behaviors" => state.settings.behaviors = nested,
            "__plugins" => {
                let plugins = nested.get("plugins").cloned().unwrap_or(nested);
                state.settings.plugins = serde_json::from_value(plugins)
                    .map_err(|e| format!("invalid plugins selection: {e}"))?;
            }
            "" => return Err("module_id is required".into()),
            _ => {
                if !state.modules.iter().any(|d| d.id == module_id) {
                    return Err(format!("Unknown module {module_id}"));
                }
                state.settings.modules.insert(module_id.to_owned(), nested);
            }
        }
        let config = state.settings.module_config();
        drop(state);
        self.engine.update(&config);
        Ok(())
    }

    /// `status`/`hello`/`config` payloads — rebuilt every tick; the link
    /// dedupes, so the cost is a small JSON build per second.
    fn push_panel_state(&self) {
        let Ok(state) = self.state.lock() else {
            return;
        };
        let session = state.session.clone();
        let settings = state.settings.clone();
        let modules = state.modules.clone();
        let statistics = state.statistics.clone();
        let running = self.engine.is_running();
        let activity = self.engine.report().unwrap_or_else(|| {
            if running {
                "Running".into()
            } else {
                "Idle".into()
            }
        });
        let needs_human = self.health.needs_human();
        drop(state);
        let status = botd_panel::status_body(
            self.health.phase(),
            running,
            &activity,
            session.as_ref(),
            statistics.as_ref(),
            &settings,
            needs_human.as_deref(),
        );

        if let Some(stream) = &self.stream {
            let sequence = self.stream_seq.fetch_add(1, Ordering::Relaxed) + 1;
            stream.publish(serde_json::json!({
                "schema": 1,
                "type": "bot.status",
                "bot_id": self.config.bot.id,
                "tenant": self.config.bot.tenant,
                "epoch": self.epoch(),
                "sequence": sequence,
                "observed_at_unix_ms": statistics::now_ms(),
                "status": status.clone(),
            }));
        }

        if self.config.panel.is_some() {
            self.panel.set_hello(botd_panel::hello_body(
                &self.config,
                session.as_ref(),
                &modules,
                &settings,
            ));
            self.panel
                .push_config(botd_panel::config_body(&settings, &modules));
            self.panel.set_status(status);
        }
    }

    /// Periodic Backpage probe (§2.4), off-thread: `check_sid` every
    /// `deps.sid_check` — an expired dosid means re-login in-process
    /// (password auth) or a `needs_human` hold (session auth); plus the
    /// 60 s hangar-balance reconcile while the sid is still valid.
    fn backpage_probe(self: &Arc<Self>) {
        let (session, statistics, sid_due, balance_due) = {
            let Ok(mut state) = self.state.lock() else {
                return;
            };
            let sid_due = state.last_sid_check.elapsed() >= self.deps.sid_check;
            let balance_due = state.last_balance_fetch.elapsed() >= Duration::from_secs(60);
            if !sid_due && !balance_due {
                return;
            }
            if sid_due {
                state.last_sid_check = Instant::now();
            }
            if balance_due {
                state.last_balance_fetch = Instant::now();
            }
            match (state.session.clone(), state.statistics.clone()) {
                (Some(session), Some(stats)) => (session, stats, sid_due, balance_due),
                _ => return,
            }
        };
        let daemon = self.clone();
        std::thread::Builder::new()
            .name("backpage-probe".into())
            .spawn(move || {
                if sid_due && daemon.sid_expired(&session) {
                    daemon.on_sid_expired();
                    return;
                }
                if balance_due
                    && let Ok(balances) = statistics::fetch_balances(session)
                    && let Ok(mut stats) = statistics.lock()
                {
                    stats.reconcile(balances, "backpage", statistics::now_ms());
                }
            })
            .ok();
    }

    /// `check_sid` → `true` only when Backpage says the dosid is dead.
    /// Transport and inconclusive statuses do not count — never trigger a
    /// re-login on a guess.
    fn sid_expired(&self, session: &Session) -> bool {
        let session = session.clone();
        let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        else {
            return false;
        };
        runtime.block_on(async {
            match xenomit_backpage::BackpageClient::new(session) {
                Ok(client) => matches!(client.check_sid().await, SidStatus::Invalid),
                Err(_) => false,
            }
        })
    }

    /// Dead dosid (§2.4): password auth re-logins in-process through the
    /// normal `HostAction::Refresh` path; `auth = "session"` cannot mint a
    /// new sid on its own, so it parks on `needs_human` rather than
    /// hammering the login endpoints with a dead credential.
    fn on_sid_expired(&self) {
        if self.config.account.auth == AuthMode::Password {
            warn!("sid expired — re-login in-process");
            self.log_stat("sid expired — refreshing");
            self.host_actions.handle(HostAction::Refresh);
        } else {
            warn!("sid expired — session auth cannot self-refresh; holding");
            self.log_stat("sid expired — needs a fresh dosid");
            self.stop_module();
            self.detach();
            if let Ok(mut state) = self.state.lock() {
                state.hold = true;
                state.resume_module = false;
            }
            self.health.set_needs_human(Some("session_expired".into()));
            self.health
                .set_last_error(Some("dosid expired — supply a fresh one".into()));
        }
    }

    /// Stopped-state general rules (refresh timers etc.) — off-thread.
    fn tick_stopped(&self) {
        let (client, config) = {
            let Ok(mut state) = self.state.lock() else {
                return;
            };
            if self.engine.is_running()
                || state.last_stopped_tick.elapsed() < self.deps.tick.max(Duration::from_secs(1))
            {
                return;
            }
            state.last_stopped_tick = Instant::now();
            match state.client.clone() {
                Some(client) => (client, state.settings.module_config()),
                None => return,
            }
        };
        let engine = self.engine.clone();
        std::thread::Builder::new()
            .name("stopped-tick".into())
            .spawn(move || {
                let _ = engine.tick_stopped(client, &config, None);
            })
            .ok();
    }

    // ------------------------------------------------------------------
    // Drain
    // ------------------------------------------------------------------

    fn drain(&self, reason: DrainReason) -> ExitCode {
        match reason {
            DrainReason::Abort => {
                warn!("abort requested — exiting immediately");
                self.detach();
                self.panel.shutdown();
                ExitCode::Crash
            }
            DrainReason::Error(error) => {
                // supervise() errors reach here without ever touching the
                // "daemon failed" log in run() — a fenced pod's log would
                // just stop mid-run and the cause of the exit (67) would
                // be invisible on the pod's own surface.
                error!(%error, "fatal — exiting");
                self.health.set_last_error(Some(error.to_string()));
                self.health
                    .set_needs_human(error.needs_human().map(str::to_owned));
                // Fenced pods write nothing — every store op would be
                // rejected anyway and the successor owns the account now.
                let fenced = matches!(error, BotdError::Fenced(_));
                if !fenced {
                    // Other failures still log out cleanly; a best-effort
                    // degraded checkpoint preserves what crash recovery
                    // would otherwise lose.
                    self.write_exit_checkpoint(fenced);
                }
                self.detach();
                self.panel.shutdown();
                error.exit_code()
            }
            DrainReason::Clean(reason) => {
                // Pre-activation exits and legacy paths: if a worker is
                // still up, run it through the drain protocol anyway.
                if self.epoch() > 0 && self.engine.is_running() {
                    self.begin_handoff(self.configured_drain(reason));
                    let deadline = Instant::now()
                        + Duration::from_secs(self.config.handoff.hard_deadline_s)
                        + Duration::from_secs(15);
                    let drained = loop {
                        if let Some(drained) = self.handoff.take_drained() {
                            break Some(Box::new(drained));
                        }
                        if !self.engine.is_running() || Instant::now() >= deadline {
                            break None;
                        }
                        std::thread::sleep(Duration::from_millis(25));
                    };
                    return self.finish_handoff(drained, reason);
                }
                info!(%reason, "clean shutdown");
                self.set_phase(Phase::Draining);
                self.health.set_ready(false);
                self.heartbeat.set_worker_expected(false);
                self.detach();
                self.panel.shutdown();
                ExitCode::Clean
            }
            DrainReason::Handoff { drained, reason } => self.finish_handoff(drained, &reason),
        }
    }

    /// `Snapshot → Logout → Drained → exit 0` (docs/03 §3.2/§3.7). The
    /// worker already stopped; write the epoch-conditional snapshot, flush
    /// the outbox, drop the session, then tell the control plane.
    fn finish_handoff(&self, drained: Option<Box<Drained>>, reason: &str) -> ExitCode {
        self.stop_module();
        self.log_stat("botd drained");
        self.health.set_ready(false);
        self.heartbeat.set_worker_expected(false);
        // Only activated pods (epoch > 0) may touch the epoch-conditional
        // store or report Drained.
        if self.epoch() == 0 {
            self.detach();
            self.panel.shutdown();
            return ExitCode::Clean;
        }
        self.flush_outbox();
        self.set_phase(Phase::Snapshotting);
        let (quality, checkpoint) = match drained {
            Some(drained) => (
                snapshot::wire_quality(drained.quality),
                Some(drained.checkpoint),
            ),
            // No worker answer — stats-only snapshot is degraded by
            // construction.
            None => (SnapshotQuality::Degraded, None),
        };
        let snapshot = self.assemble_snapshot(checkpoint, snapshot::wire_reason(reason), quality);
        let snapshot_id = match snapshot::block_on(self.deps.store.put(&snapshot)) {
            Ok(id) => Some(id),
            Err(StoreError::Fenced(fenced)) => {
                warn!(%fenced, "drain snapshot fenced — exiting 67");
                self.detach();
                self.panel.shutdown();
                return ExitCode::Fenced;
            }
            Err(error) => {
                // Snapshot writes are best-effort: the successor restores
                // the latest checkpoint (or nothing) but must not be blocked
                // by our failure (docs/03 §3.5).
                warn!(%error, "drain snapshot write failed — logging out anyway");
                None
            }
        };
        self.set_phase(Phase::LoggingOut);
        self.detach();
        let report = DrainedReport {
            snapshot_id,
            clean: quality == SnapshotQuality::Clean,
        };
        if let Err(error) = self.deps.control.report_drained(report) {
            warn!(%error, "Drained report failed");
        }
        self.panel.shutdown();
        info!("drain complete — exit 0");
        ExitCode::Clean
    }

    /// Best-effort `Checkpoint`-quality write on error exits so a restart
    /// (or a manually triggered restore) does not lose the last interval.
    fn write_exit_checkpoint(&self, fenced: bool) {
        if fenced || self.epoch() == 0 {
            return;
        }
        let snapshot = self.assemble_snapshot(
            None,
            snapshot::wire_reason("eviction"),
            SnapshotQuality::Degraded,
        );
        if let Err(error) = snapshot::block_on(self.deps.store.put(&snapshot)) {
            warn!(%error, "exit checkpoint write failed");
        }
    }

    /// Wait for the panel to acknowledge the stat outbox — bounded by
    /// `outbox_flush_budget`. Events past the budget stay journaled; the
    /// successor replays them from the snapshot tail or journal (docs/03
    /// §3.6) so an early exit is never data loss.
    fn flush_outbox(&self) {
        if self.config.panel.is_none() {
            return;
        }
        let deadline = Instant::now() + self.deps.outbox_flush_budget;
        loop {
            let unacked = self
                .statistics()
                .and_then(|stats| stats.lock().ok().map(|s| s.unacked_len()));
            match unacked {
                Some(0) | None => return,
                Some(_) if Instant::now() >= deadline => {
                    warn!(
                        unacked = unacked.unwrap_or(0),
                        "outbox flush budget exhausted"
                    );
                    return;
                }
                _ => {}
            }
            self.drain_stat_acks();
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    fn log_stat(&self, message: &str) {
        let statistics = self.state.lock().ok().and_then(|s| s.statistics.clone());
        if let Some(stats) = statistics {
            self.log_stat_locked(&stats, message);
        }
    }

    fn log_stat_locked(&self, statistics: &SharedStats, message: &str) {
        if let Ok(mut stats) = statistics.lock() {
            stats.log(message, "botd", statistics::now_ms());
        }
    }
}

fn secret_error(error: SecretError) -> BotdError {
    match error {
        SecretError::Unavailable(message) => BotdError::Unavailable(message),
        SecretError::Invalid(message) => BotdError::Config(message),
    }
}

// ---------------------------------------------------------------------
// Signal handlers
// ---------------------------------------------------------------------

#[cfg(unix)]
fn install_signal_handlers(runtime: &tokio::runtime::Runtime, signals: Arc<Signals>) {
    use tokio::signal::unix::{SignalKind, signal};
    runtime.spawn(async move {
        let Ok(mut terminate) = signal(SignalKind::terminate()) else {
            return;
        };
        let Ok(mut quit) = signal(SignalKind::quit()) else {
            return;
        };
        let Ok(mut interrupt) = signal(SignalKind::interrupt()) else {
            return;
        };
        loop {
            tokio::select! {
                _ = terminate.recv() => signals.request_shutdown(),
                _ = interrupt.recv() => signals.request_shutdown(),
                _ = quit.recv() => signals.request_abort(),
            }
        }
    });
}

#[cfg(not(unix))]
fn install_signal_handlers(_runtime: &tokio::runtime::Runtime, signals: Arc<Signals>) {
    install_signal_handlers_std(signals);
}

#[cfg(not(unix))]
fn install_signal_handlers_std(signals: Arc<Signals>) {
    std::thread::Builder::new()
        .name("signal-watch".into())
        .spawn(move || {
            // ctrlc handling via tokio would need a runtime; std has none —
            // use a one-shot handler registered with the OS instead.
            ctrlc_fallback(signals);
        })
        .ok();
}

#[cfg(unix)]
fn install_signal_handlers_std(signals: Arc<Signals>) {
    // Fallback when no tokio runtime exists: spawn a minimal runtime thread.
    std::thread::Builder::new()
        .name("signal-watch".into())
        .spawn(move || {
            let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            runtime.block_on(async move {
                install_signal_handlers_via_tokio(signals).await;
            });
        })
        .ok();
}

#[cfg(unix)]
async fn install_signal_handlers_via_tokio(signals: Arc<Signals>) {
    use tokio::signal::unix::{SignalKind, signal};
    let Ok(mut terminate) = signal(SignalKind::terminate()) else {
        return;
    };
    let Ok(mut quit) = signal(SignalKind::quit()) else {
        return;
    };
    let Ok(mut interrupt) = signal(SignalKind::interrupt()) else {
        return;
    };
    loop {
        tokio::select! {
            _ = terminate.recv() => signals.request_shutdown(),
            _ = interrupt.recv() => signals.request_shutdown(),
            _ = quit.recv() => signals.request_abort(),
        }
    }
}

#[cfg(not(unix))]
fn ctrlc_fallback(signals: Arc<Signals>) {
    // Windows console: SetConsoleCtrlHandler via the `ctrlc` crate is not a
    // dependency; fall back to tokio's ctrl_c on a scratch runtime.
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return;
    };
    runtime.block_on(async move {
        #[cfg(windows)]
        {
            // CTRL_BREAK is the only console event a supervisor can target
            // at a specific process group (GenerateConsoleCtrlEvent); the
            // fleet harness uses it for a graceful per-bot stop. A second
            // break escalates to abort via `request_shutdown`.
            let mut ctrl_break = tokio::signal::windows::ctrl_break().ok();
            tokio::select! {
                _ = tokio::signal::ctrl_c() => signals.request_shutdown(),
                _ = async {
                    match ctrl_break.as_mut() {
                        Some(break_signal) => {
                            let _ = break_signal.recv().await;
                        }
                        None => std::future::pending::<()>().await,
                    }
                } => signals.request_shutdown(),
            }
        }
        #[cfg(not(windows))]
        {
            let _ = tokio::signal::ctrl_c().await;
            signals.request_shutdown();
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::sync::atomic::AtomicUsize;

    use xenomit_game_api::{
        ActionResult, ApiError, ClientStatus, FunctionCall, GameSnapshot, Inspection,
        Result as ApiResult,
    };

    use crate::secrets::FileSecretResolver;

    /// Ready flag is controllable so tests can drop readiness mid-run.
    struct FakeClient {
        ready: AtomicBool,
    }

    impl FakeClient {
        fn ready() -> Arc<Self> {
            Arc::new(Self {
                ready: AtomicBool::new(true),
            })
        }
    }

    impl GameClient for FakeClient {
        fn status(&self) -> ApiResult<ClientStatus> {
            Ok(ClientStatus {
                backend: "fake".into(),
                connected: true,
                ready: self.ready.load(Ordering::Relaxed),
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

    /// Always returns the same client; `calls` tracks attach attempts.
    struct FakeConnector {
        calls: AtomicUsize,
        client: Arc<FakeClient>,
    }

    impl FakeConnector {
        fn new(client: Arc<FakeClient>) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                client,
            }
        }
    }

    impl GameConnector for FakeConnector {
        fn connect(
            &self,
            _session: &Session,
            _statistics: SharedStats,
            _game_endpoint: Option<&str>,
        ) -> Result<Arc<dyn GameClient>, BotdError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(self.client.clone())
        }
    }

    #[derive(Clone, Copy)]
    enum AuthOutcome {
        Ok,
        Reject,
        /// First `arm()` is transient-unavailable, the retry succeeds.
        TransientArm,
    }

    struct FakeAuth {
        outcome: AuthOutcome,
        arm_calls: AtomicUsize,
    }

    impl FakeAuth {
        fn new(outcome: AuthOutcome) -> Self {
            Self {
                outcome,
                arm_calls: AtomicUsize::new(0),
            }
        }
    }

    impl Authenticator for FakeAuth {
        fn arm(&self) -> Result<(), SecretError> {
            if matches!(self.outcome, AuthOutcome::TransientArm)
                && self.arm_calls.fetch_add(1, Ordering::Relaxed) == 0
            {
                return Err(SecretError::Unavailable("secrets not projected yet".into()));
            }
            Ok(())
        }
        fn authenticate(&self) -> Result<Session, BotdError> {
            match self.outcome {
                AuthOutcome::Ok | AuthOutcome::TransientArm => {
                    let mut session = Session::new("fake-dosid", "int1");
                    // Point periodic balance fetches at a dead port so the
                    // reconcile thread fails fast instead of doing real I/O.
                    session.set_instance_base(Some("http://127.0.0.1:1".into()));
                    Ok(session)
                }
                AuthOutcome::Reject => Err(BotdError::CredentialsRejected("rejected".into())),
            }
        }
    }

    fn test_config(dir: &Path) -> BotdConfig {
        serde_json::from_value(serde_json::json!({
            "schema": 1,
            "bot": {"id": "bot_t", "tenant": "t"},
            "account": {
                "server": "int1.darkorbit.com",
                "auth": "session",
                "credentials": {"file": "unused-by-fake-auth"}
            },
            "client": {"kind": "packet"},
            "state_dir": dir
        }))
        .unwrap()
    }

    fn fake_deps(
        dir: &Path,
        authenticator: Arc<dyn Authenticator>,
        connector: Arc<dyn GameConnector>,
    ) -> Deps {
        Deps {
            resolver: Arc::new(FileSecretResolver),
            authenticator,
            connector,
            control: Arc::new(StandaloneControl::new()),
            store: Arc::new(LocalSnapshotStore::new(dir.to_path_buf())),
            engine_factory: Arc::new(|handler, handoff| {
                Arc::new(
                    PlannerRuntime::builtin_only()
                        .with_host_action_handler(handler)
                        .with_handoff_link(handoff),
                ) as Arc<dyn BehaviorRuntime>
            }),
            tick: Duration::from_millis(10),
            arm_retry: Duration::from_millis(150),
            reconnect_delay: Duration::from_millis(20),
            // Effectively off: fake sessions point at a dead port and unit
            // tests finish long before this elapses.
            sid_check: Duration::from_secs(60),
            health_bind: None,
            health_rt: None,
            handle_signals: false,
            outbox_flush_budget: Duration::from_millis(500),
        }
    }

    fn spawn_run(daemon: &Arc<Daemon>) -> std::thread::JoinHandle<ExitCode> {
        let daemon = daemon.clone();
        std::thread::spawn(move || daemon.run())
    }

    fn wait_for_phase(daemon: &Daemon, phase: Phase) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while daemon.phase() != phase {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for phase {phase}"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn lifecycle_reaches_run_and_drains_cleanly() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = Daemon::new(
            test_config(dir.path()),
            fake_deps(
                dir.path(),
                Arc::new(FakeAuth::new(AuthOutcome::Ok)),
                Arc::new(FakeConnector::new(FakeClient::ready())),
            ),
        );
        let signals = daemon.signals();
        let handle = spawn_run(&daemon);
        wait_for_phase(&daemon, Phase::Run);
        assert!(daemon.session().is_some());
        signals.request_shutdown();
        assert_eq!(handle.join().unwrap(), ExitCode::Clean);
        assert_eq!(daemon.phase(), Phase::LoggingOut);
    }

    #[test]
    fn transient_arm_failure_retries_until_resolved() {
        let dir = tempfile::tempdir().unwrap();
        let auth = Arc::new(FakeAuth::new(AuthOutcome::TransientArm));
        let daemon = Daemon::new(
            test_config(dir.path()),
            fake_deps(
                dir.path(),
                auth.clone(),
                Arc::new(FakeConnector::new(FakeClient::ready())),
            ),
        );
        let signals = daemon.signals();
        let handle = spawn_run(&daemon);
        wait_for_phase(&daemon, Phase::Run);
        assert!(auth.arm_calls.load(Ordering::Relaxed) >= 2);
        signals.request_shutdown();
        assert_eq!(handle.join().unwrap(), ExitCode::Clean);
    }

    #[test]
    fn rejected_credentials_exit_65_with_needs_human() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = Daemon::new(
            test_config(dir.path()),
            fake_deps(
                dir.path(),
                Arc::new(FakeAuth::new(AuthOutcome::Reject)),
                Arc::new(FakeConnector::new(FakeClient::ready())),
            ),
        );
        assert_eq!(daemon.run(), ExitCode::CredentialsRejected);
        assert_eq!(
            daemon.health_state().needs_human().as_deref(),
            Some("credentials")
        );
    }

    #[test]
    fn lost_readiness_detaches_and_reattaches() {
        let dir = tempfile::tempdir().unwrap();
        let client = FakeClient::ready();
        let connector = Arc::new(FakeConnector::new(client.clone()));
        let daemon = Daemon::new(
            test_config(dir.path()),
            fake_deps(
                dir.path(),
                Arc::new(FakeAuth::new(AuthOutcome::Ok)),
                connector.clone(),
            ),
        );
        let signals = daemon.signals();
        let handle = spawn_run(&daemon);
        wait_for_phase(&daemon, Phase::Run);
        assert_eq!(connector.calls.load(Ordering::Relaxed), 1);
        // Drop the link; the supervisor must detach and re-login.
        client.ready.store(false, Ordering::Relaxed);
        let deadline = Instant::now() + Duration::from_secs(10);
        while connector.calls.load(Ordering::Relaxed) < 2 {
            assert!(Instant::now() < deadline, "daemon never re-attached");
            std::thread::sleep(Duration::from_millis(5));
        }
        client.ready.store(true, Ordering::Relaxed);
        signals.request_shutdown();
        assert_eq!(handle.join().unwrap(), ExitCode::Clean);
    }
}
