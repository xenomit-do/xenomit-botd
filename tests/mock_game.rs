//! End-to-end integration against `xenomit-mock-game` (`docs/02-headless-bot.md`
//! §7). The mock is embedded in-process — listeners run on a private tokio
//! runtime thread — so no external server or env vars are needed:
//!
//! ```sh
//! cargo test -p xenomit-botd --test mock_game
//! ```
//!
//! `testing.backpage_base_url` redirects every Backpage call (sid validation,
//! flashvars, hangar balances) at the mock; `testing.game_endpoint` skips the
//! `maps.php` discovery and points the packet client at the mock socket.
//!
//! The suite is serialized through [`SUITE`]: daemons in one process share
//! the runtime's `set_backpage_session` global, and parallel mocks only add
//! flakiness.
mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use xenomit_botd::control::StandaloneControl;
use xenomit_botd::daemon::{Daemon, Deps};
use xenomit_botd::error::ExitCode;
use xenomit_botd::state::Phase;
use xenomit_mock_game::state::SessionCmd;
use xenomit_packet_client::session::{LoginInfo, Session as PacketSession};

use common::*;

/// One daemon per test — serialize the suite.
static SUITE: Mutex<()> = Mutex::new(());

/// Lock the suite, ignoring poison from a panicked sibling test.
fn suite() -> std::sync::MutexGuard<'static, ()> {
    SUITE.lock().unwrap_or_else(|e| e.into_inner())
}

fn standalone_deps(config: &xenomit_botd::config::BotdConfig, health: bool) -> Deps {
    daemon_deps(
        config,
        health,
        Arc::new(StandaloneControl::new()),
        &config.state_dir.clone(),
    )
}

// ------------------------------------------------------------------------
// Login → attach → run, statistics JSONL, health endpoints
// ------------------------------------------------------------------------

#[test]
fn login_attach_run_writes_stats_jsonl_and_serves_health() {
    let _guard = suite();
    let mock = Mock::start(|_| {});
    let dir = tempfile::tempdir().unwrap();
    let config = fixture(
        dir.path(),
        &mock,
        &DaemonOpts::password("bot", "bot-pass").autostart("kill_collect"),
    );
    let daemon = Daemon::new(config.clone(), standalone_deps(&config, true));
    let signals = daemon.signals();
    let health = daemon.health_state();
    let handle = spawn_run(&daemon);

    wait_phase(&daemon, Phase::Run);
    assert!(daemon.session().is_some(), "no Backpage session");
    assert_eq!(mock.game_logins(), 1, "expected one game login");
    assert!(daemon.engine().is_running(), "autostart module not running");

    // Health server bound to an ephemeral port — discover it.
    wait_until(Duration::from_secs(10), "health bind", || {
        health.bound_addr().is_some()
    });
    let bound = health.bound_addr().unwrap();
    assert_eq!(http_get(bound, "/startupz").0, 200);
    assert_eq!(http_get(bound, "/readyz").0, 200);
    assert_eq!(http_get(bound, "/livez").0, 200);
    assert_eq!(http_get(bound, "/nope").0, 404);

    // Statistics JSONL under `state_dir/statistics` carries real events.
    let stats_dir = dir.path().join("statistics");
    wait_until(Duration::from_secs(15), "statistics journal", || {
        stats_dir
            .read_dir()
            .map(|mut d| {
                d.any(|entry| {
                    entry.is_ok_and(|e| {
                        e.path().extension().is_some_and(|ext| ext == "jsonl")
                            && e.metadata().is_ok_and(|m| m.len() > 0)
                    })
                })
            })
            .unwrap_or(false)
    });
    let journal = stats_dir
        .read_dir()
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.extension().is_some_and(|ext| ext == "jsonl"))
        .unwrap();
    let lines: Vec<String> = std::fs::read_to_string(&journal)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    assert!(!lines.is_empty(), "empty statistics journal");
    let joined = lines.join("\n");
    assert!(
        joined.contains("session attached"),
        "journal missing attach event: {joined}"
    );
    // Every line parses as a JSON statistics event.
    for line in &lines {
        let event: serde_json::Value = serde_json::from_str(line).unwrap();
        assert!(event["at"].is_u64(), "bad event {line}");
        assert!(event["source"].is_string(), "bad event {line}");
    }
    // The session's statistics journal carries sequenced events for the
    // control-plane outbox (docs/03 §3.6).
    let unacked = daemon
        .statistics()
        .map(|stats| stats.lock().unwrap().unacked_len())
        .unwrap_or(0);
    assert!(unacked > 0, "statistics outbox never recorded an event");

    signals.request_shutdown();
    assert_eq!(handle.join().unwrap(), ExitCode::Clean);
    assert_eq!(daemon.phase(), Phase::LoggingOut);
}

// ------------------------------------------------------------------------
// Exit codes 65 / 66 / 68
// ------------------------------------------------------------------------

#[test]
fn rejected_credentials_exit_65() {
    let _guard = suite();
    let mock = Mock::start(|config| {
        config.faults.backpage.reject_credentials = true;
    });
    let dir = tempfile::tempdir().unwrap();
    let config = fixture(dir.path(), &mock, &DaemonOpts::password("bot", "bot-pass"));
    let daemon = Daemon::new(config.clone(), standalone_deps(&config, false));
    let result = daemon.run();
    assert_eq!(result, ExitCode::CredentialsRejected);
    assert_eq!(result.code(), 65);
    assert_eq!(
        daemon.health_state().needs_human().as_deref(),
        Some("credentials")
    );
}

#[test]
fn captcha_wall_exit_66() {
    let _guard = suite();
    let mock = Mock::start(|config| {
        config.faults.backpage.captcha = true;
    });
    let dir = tempfile::tempdir().unwrap();
    let config = fixture(dir.path(), &mock, &DaemonOpts::password("bot", "bot-pass"));
    let daemon = Daemon::new(config.clone(), standalone_deps(&config, false));
    let result = daemon.run();
    assert_eq!(result, ExitCode::CaptchaRequired);
    assert_eq!(result.code(), 66);
    assert_eq!(
        daemon.health_state().needs_human().as_deref(),
        Some("captcha")
    );
}

#[test]
fn version_mismatch_exit_68() {
    let _guard = suite();
    let mock = Mock::start(|config| {
        config.faults.game.version_mismatch = true;
    });
    let dir = tempfile::tempdir().unwrap();
    let config = fixture(dir.path(), &mock, &DaemonOpts::password("bot", "bot-pass"));
    let daemon = Daemon::new(config.clone(), standalone_deps(&config, false));
    let result = daemon.run();
    assert_eq!(result, ExitCode::VersionMismatch);
    assert_eq!(result.code(), 68);
    assert_eq!(
        daemon.health_state().needs_human().as_deref(),
        Some("version")
    );
}

// ------------------------------------------------------------------------
// SID expiry → in-process re-login (password) / needs-human hold (session)
// ------------------------------------------------------------------------

#[test]
fn expired_sid_relogs_in_process_and_resumes_module() {
    let _guard = suite();
    let mock = Mock::start(|config| {
        // sid dies a few seconds after issue — long enough that the
        // refreshed dosid outlives the test's assertions.
        config.backpage.session_ttl_secs = 20;
    });
    let dir = tempfile::tempdir().unwrap();
    let config = fixture(
        dir.path(),
        &mock,
        &DaemonOpts::password("bot", "bot-pass").autostart("kill_collect"),
    );
    let daemon = Daemon::new(config.clone(), standalone_deps(&config, true));
    let signals = daemon.signals();
    let handle = spawn_run(&daemon);

    wait_phase(&daemon, Phase::Run);
    let first_sid = daemon.session().unwrap().sid.clone();
    assert!(!first_sid.is_empty());

    // The probe must see the dead sid and refresh — a second Backpage
    // login plus a second game login follow.
    wait_until(Duration::from_secs(60), "in-process re-login", || {
        mock.game_logins() >= 2
            && daemon
                .session()
                .is_some_and(|session| session.sid != first_sid)
    });
    assert!(!handle.is_finished(), "daemon exited instead of refreshing");
    wait_phase(&daemon, Phase::Run);
    wait_until(
        Duration::from_secs(20),
        "module resume after refresh",
        || daemon.engine().is_running(),
    );
    let second_sid = daemon.session().unwrap().sid;
    assert_ne!(second_sid, first_sid, "re-login must mint a new dosid");
    // No login storm: exactly two game logins (initial + refresh).
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(mock.game_logins(), 2);

    signals.request_shutdown();
    assert_eq!(handle.join().unwrap(), ExitCode::Clean);
}

#[test]
fn expired_sid_session_auth_holds_for_human() {
    let _guard = suite();
    let mock = Mock::start(|config| {
        config.backpage.session_ttl_secs = 6;
    });
    // Pre-mint the dosid (ttl ticks from issue).
    let sid = mock.shared.issue_sid(&mock.account());
    let dir = tempfile::tempdir().unwrap();
    let config = fixture(dir.path(), &mock, &DaemonOpts::dosid(&sid));
    let daemon = Daemon::new(config.clone(), standalone_deps(&config, true));
    let signals = daemon.signals();
    let handle = spawn_run(&daemon);

    wait_phase(&daemon, Phase::Run);
    assert_eq!(mock.game_logins(), 1);

    // Session auth cannot mint a new dosid → hold on needs_human.
    wait_until(Duration::from_secs(45), "needs_human hold", || {
        daemon.health_state().needs_human().as_deref() == Some("session_expired")
    });
    assert!(!handle.is_finished(), "daemon exited instead of holding");
    // No re-login hammering: still exactly one game login.
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(mock.game_logins(), 1);

    signals.request_shutdown();
    assert_eq!(handle.join().unwrap(), ExitCode::Clean);
}

// ------------------------------------------------------------------------
// Kick on second login → rate-limited re-attach, no tight login loop
// ------------------------------------------------------------------------

#[test]
fn second_login_kick_reattaches_once_without_login_storm() {
    let _guard = suite();
    let mock = Mock::start(|_| {});
    let dir = tempfile::tempdir().unwrap();
    let config = fixture(
        dir.path(),
        &mock,
        &DaemonOpts::password("bot", "bot-pass").autostart("kill_collect"),
    );
    let daemon = Daemon::new(config.clone(), standalone_deps(&config, true));
    let signals = daemon.signals();
    let handle = spawn_run(&daemon);

    wait_phase(&daemon, Phase::Run);
    assert_eq!(mock.game_logins(), 1);
    wait_until(Duration::from_secs(15), "module running", || {
        daemon.engine().is_running()
    });

    // A second session logs in on the same account — the mock kicks ours.
    let squatter_sid = mock.shared.issue_sid(&mock.account());
    let game = mock.game.clone();
    let uid = mock.account().user_id;
    let squatter = std::thread::spawn(move || {
        let login = LoginInfo::new(uid, squatter_sid);
        let mut session =
            PacketSession::connect_and_login(&game, &login).expect("squatter login failed");
        // Hold until kicked by the daemon's re-login.
        let end = Instant::now() + Duration::from_secs(60);
        while Instant::now() < end {
            if session.poll(Duration::from_millis(200)).is_err() {
                break;
            }
        }
    });

    // Squatter's login lands (2); the daemon then re-logins once (3).
    let squatter_at = wait_logins(&mock, 2);
    let relogin_at = wait_logins(&mock, 3);
    assert!(
        relogin_at.duration_since(squatter_at) >= Duration::from_millis(250),
        "re-login came without the reconnect delay"
    );
    // Still running — and the module came back on the fresh session.
    assert!(!handle.is_finished(), "daemon exited after kick");
    wait_phase(&daemon, Phase::Run);
    wait_until(Duration::from_secs(20), "module resume after kick", || {
        daemon.engine().is_running()
    });
    let new_sid = daemon.session().map(|s| s.sid);
    assert!(new_sid.is_some(), "no session after re-attach");

    // No tight loop: logins stay at 3 and the squatter was kicked off.
    std::thread::sleep(Duration::from_secs(3));
    assert_eq!(mock.game_logins(), 3, "login storm after kick");
    squatter.join().unwrap();

    // A manual drop kicks the same re-attach path once more.
    let cmd = mock
        .shared
        .sessions
        .lock()
        .unwrap()
        .get(&uid)
        .map(|e| e.cmd.clone());
    if let Some(cmd) = cmd {
        let _ = cmd.try_send(SessionCmd::Drop {
            reason: "test drop".into(),
        });
        wait_logins(&mock, 4);
        wait_until(Duration::from_secs(20), "second resume", || {
            daemon.engine().is_running()
        });
        std::thread::sleep(Duration::from_secs(2));
        assert_eq!(mock.game_logins(), 4, "login storm after drop");
    }

    signals.request_shutdown();
    assert_eq!(handle.join().unwrap(), ExitCode::Clean);
}

/// Wait until `mock.game_logins()` reaches `n`; returns when it happened.
fn wait_logins(mock: &Mock, n: u64) -> Instant {
    let deadline = Instant::now() + Duration::from_secs(60);
    while mock.game_logins() < n {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {n} game logins (at {})",
            mock.game_logins()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    Instant::now()
}
