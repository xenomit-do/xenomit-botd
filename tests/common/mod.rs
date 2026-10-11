//! Shared harness for `xenomit-botd` integration tests against
//! `xenomit-mock-game` (`docs/02-headless-bot.md` §7). The mock is embedded
//! in-process — listeners run on a private tokio runtime thread — so no
//! external server or env vars are needed.
#![allow(dead_code)]
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use xenomit_bot_runtime::PlannerRuntime;
use xenomit_botd::config::{AuthMode, BotdConfig};
use xenomit_botd::control::ControlPlane;
use xenomit_botd::daemon::{Daemon, Deps, EngineFactory, PacketConnector};
use xenomit_botd::error::ExitCode;
use xenomit_botd::secrets::FileSecretResolver;
use xenomit_botd::session::HeadlessSessionProvider;
use xenomit_botd::snapshot::LocalSnapshotStore;
use xenomit_botd::state::Phase;
use xenomit_game_api::BehaviorRuntime;
use xenomit_mock_game::{Config, MockServer, Shared};

/// Mock on its own runtime thread; stops when dropped.
pub struct Mock {
    pub shared: Arc<Shared>,
    pub backpage: String,
    pub game: String,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl Mock {
    pub fn start(configure: impl FnOnce(&mut Config)) -> Self {
        let mut config = Config::default();
        configure(&mut config);
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let server = runtime.block_on(MockServer::start(config)).unwrap();
        let shared = server.shared.clone();
        let backpage = server.backpage_url();
        let game = server.game_addr();
        let (stop, rx) = tokio::sync::oneshot::channel::<()>();
        let join = std::thread::spawn(move || {
            runtime.block_on(async move {
                let _ = rx.await;
                server.shutdown().await;
            });
        });
        Self {
            shared,
            backpage,
            game,
            stop: Some(stop),
            join: Some(join),
        }
    }

    pub fn account(&self) -> xenomit_mock_game::config::Account {
        self.shared.cfg.backpage.accounts[0].clone()
    }

    /// Cumulative game-login count — the split-brain tripwire: a correctly
    /// fenced handoff logs in once per pod, never concurrently.
    pub fn game_logins(&self) -> u64 {
        self.shared
            .game_logins
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Drop for Mock {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

/// Production-shaped deps, scaled-down timings; the real
/// `HeadlessSessionProvider`/`PacketConnector`/`PlannerRuntime` are used so
/// the tests exercise the production attach path. `store_root` may differ
/// from `state_dir` to model two pods sharing the fleet snapshot store
/// while keeping private journals.
pub fn daemon_deps(
    config: &BotdConfig,
    health: bool,
    control: Arc<dyn ControlPlane>,
    store_root: &Path,
) -> Deps {
    let resolver = Arc::new(FileSecretResolver);
    let authenticator = Arc::new(HeadlessSessionProvider::new(
        config.account.clone(),
        config.testing.clone(),
        resolver.clone(),
        config.network.clone(),
        None,
    ));
    let engine_factory: EngineFactory = Arc::new(|handler, handoff| {
        Arc::new(
            PlannerRuntime::builtin_only()
                .with_host_action_handler(handler)
                .with_handoff_link(handoff),
        ) as Arc<dyn BehaviorRuntime>
    });
    let (bind, runtime) = if health {
        (
            Some("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
            Some(
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .unwrap(),
            ),
        )
    } else {
        (None, None)
    };
    Deps {
        resolver: resolver.clone(),
        authenticator,
        connector: Arc::new(PacketConnector::new(config.network.clone(), resolver)),
        control,
        store: Arc::new(LocalSnapshotStore::new(store_root.to_path_buf())),
        engine_factory,
        tick: Duration::from_millis(25),
        arm_retry: Duration::from_secs(15),
        reconnect_delay: Duration::from_millis(300),
        sid_check: Duration::from_millis(250),
        health_bind: bind,
        health_rt: runtime,
        handle_signals: false,
        outbox_flush_budget: Duration::from_secs(2),
    }
}

pub struct DaemonOpts {
    pub auth: AuthMode,
    pub credentials: String,
    pub autostart_module: Option<&'static str>,
    /// `handoff.checkpoint_interval_s` — 0 disables periodic capture.
    pub checkpoint_interval_s: u64,
    pub drain_deadline_s: u64,
    pub hard_deadline_s: u64,
    pub restore_policy: &'static str,
}

impl DaemonOpts {
    pub fn password(user: &str, pass: &str) -> Self {
        Self {
            auth: AuthMode::Password,
            credentials: format!(r#"{{"username":"{user}","password":"{pass}"}}"#),
            autostart_module: None,
            checkpoint_interval_s: 2,
            drain_deadline_s: 30,
            hard_deadline_s: 60,
            restore_policy: "best_effort",
        }
    }

    pub fn dosid(sid: &str) -> Self {
        Self {
            auth: AuthMode::Session,
            credentials: format!(r#"{{"dosid":"{sid}"}}"#),
            autostart_module: None,
            checkpoint_interval_s: 2,
            drain_deadline_s: 30,
            hard_deadline_s: 60,
            restore_policy: "best_effort",
        }
    }

    pub fn autostart(mut self, module: &'static str) -> Self {
        self.autostart_module = Some(module);
        self
    }
    pub fn checkpoint_interval(mut self, secs: u64) -> Self {
        self.checkpoint_interval_s = secs;
        self
    }
    pub fn deadlines(mut self, drain_s: u64, hard_s: u64) -> Self {
        self.drain_deadline_s = drain_s;
        self.hard_deadline_s = hard_s;
        self
    }
    pub fn strict(mut self) -> Self {
        self.restore_policy = "strict";
        self
    }
}

/// One botd config + credentials file inside `dir`.
pub fn fixture(dir: &Path, mock: &Mock, opts: &DaemonOpts) -> BotdConfig {
    let credentials = dir.join("credentials.json");
    std::fs::write(&credentials, &opts.credentials).unwrap();
    // `BotSettings.working_map` defaults to "1-8" (map 20); the mock only
    // serves map 16 ("4-4"), so an autostarted kill_collect would try to
    // portal-travel to 20 and die on "no accessible route". Pin the
    // working map to the mock's own map.
    let settings = match opts.autostart_module {
        Some(module) => serde_json::json!({ "active_module": module, "working_map": "4-4" }),
        None => serde_json::json!({}),
    };
    let config: BotdConfig = serde_json::from_value(serde_json::json!({
        "schema": 1,
        "bot": { "id": "bot_it", "tenant": "test" },
        "account": {
            "server": "int1.darkorbit.com",
            "auth": match opts.auth { AuthMode::Password => "password", AuthMode::Session => "session" },
            "credentials": { "file": credentials }
        },
        "client": { "kind": "packet" },
        "settings": settings,
        "autostart": opts.autostart_module.is_some(),
        "state_dir": dir,
        "health": { "bind": "127.0.0.1:0" },
        "handoff": {
            "drain_deadline_s": opts.drain_deadline_s,
            "hard_deadline_s": opts.hard_deadline_s,
            "checkpoint_interval_s": opts.checkpoint_interval_s,
            "restore_policy": opts.restore_policy
        },
        "testing": {
            "backpage_base_url": mock.backpage,
            "game_endpoint": mock.game
        }
    }))
    .unwrap();
    config.validate().unwrap();
    config
}

pub fn spawn_run(daemon: &Arc<Daemon>) -> std::thread::JoinHandle<ExitCode> {
    let daemon = daemon.clone();
    std::thread::spawn(move || daemon.run())
}

pub fn wait_until(deadline: Duration, what: &str, mut ready: impl FnMut() -> bool) {
    let end = Instant::now() + deadline;
    while !ready() {
        assert!(Instant::now() < end, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

pub fn wait_phase(daemon: &Daemon, phase: Phase) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while daemon.phase() != phase {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for phase {phase} (stuck in {:?})",
            daemon.phase()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Minimal blocking HTTP/1.0 GET — enough for health probes.
pub fn http_get(addr: SocketAddr, path: &str) -> (u16, String) {
    let mut socket = TcpStream::connect(addr).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    write!(
        socket,
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut text = String::new();
    socket.read_to_string(&mut text).unwrap();
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}
