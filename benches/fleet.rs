//! 50-bot fleet benchmark: N real `xenomit-botd` processes against one
//! embedded `xenomit-mock-game`, measuring per-process RSS/CPU (`sysinfo`),
//! login latency (spawn → `/readyz` 200), and failures.
//!
//! ```sh
//! cargo bench --bench fleet -- --bots 50 --duration-secs 300
//! ```
//!
//! Options (env vars in caps work too): `--bots` (FLEET_BOTS, 50),
//! `--duration-secs` (FLEET_DURATION_SECS, 300), `--stagger-ms`
//! (FLEET_STAGGER_MS, 40), `--sample-ms` (FLEET_SAMPLE_MS, 2000),
//! `--module` (FLEET_MODULE, `kill_collect`; `none` attaches idle),
//! `--report` (FLEET_REPORT, `<crate>/BENCHMARK.md`).
//!
//! Stop: CTRL_BREAK on Windows (children spawn with
//! `CREATE_NEW_PROCESS_GROUP`; `GenerateConsoleCtrlEvent` targets each
//! group) or SIGTERM elsewhere; `child.kill()` is the fallback.
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use xenomit_mock_game::{Config, MockServer, Shared};

#[derive(Clone)]
struct Args {
    bots: usize,
    duration: Duration,
    stagger: Duration,
    sample: Duration,
    module: Option<String>,
    report: PathBuf,
}

fn args() -> Args {
    fn pick(key: &str) -> Option<String> {
        let mut argv = std::env::args().skip(1).peekable();
        while let Some(arg) = argv.next() {
            if let Some(value) = arg.strip_prefix(key).and_then(|s| s.strip_prefix('=')) {
                return Some(value.to_string());
            }
            // `--key value` (space-separated) form.
            if arg == key
                && let Some(value) = argv.next_if(|v| !v.starts_with("--"))
            {
                return Some(value);
            }
        }
        std::env::var(
            key.trim_start_matches("--")
                .replace('-', "_")
                .to_uppercase(),
        )
        .ok()
    }
    let num = |key: &str, default: u64| {
        pick(key)
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(default)
    };
    Args {
        bots: num("--bots", 50) as usize,
        duration: Duration::from_secs(num("--duration-secs", 300)),
        stagger: Duration::from_millis(num("--stagger-ms", 40)),
        sample: Duration::from_millis(num("--sample-ms", 2000)),
        module: match pick("--module").as_deref() {
            Some("none") | Some("") => None,
            Some(name) => Some(name.to_string()),
            None => Some("kill_collect".into()),
        },
        report: pick("--report")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("BENCHMARK.md")),
    }
}

// ---------------------------------------------------------------------
// Embedded mock — same helper shape as the integration tests.
// ---------------------------------------------------------------------

struct Mock {
    shared: Arc<Shared>,
    backpage: String,
    game: String,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl Mock {
    fn start(configure: impl FnOnce(&mut Config)) -> Self {
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

// ---------------------------------------------------------------------
// One spawned botd child.
// ---------------------------------------------------------------------

struct Bot {
    index: usize,
    child: Child,
    health_addr: SocketAddr,
    spawned_at: Instant,
    ready_at: Option<Instant>,
    /// Keeps the state directory (and the botd.log) alive.
    _dir: tempfile::TempDir,
    log_path: PathBuf,
    // Metrics
    login_ms: Option<u64>,
    rss_peak: u64,
    rss_sum: u128,
    cpu_sum: f64,
    samples: u64,
    exit: Option<i32>,
    failure: Option<String>,
}

fn botd_exe() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_xenomit-botd"))
}

/// Reserve an ephemeral health port (bind → read → release).
fn free_health_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

fn write_config(
    dir: &Path,
    index: usize,
    backpage: &str,
    game: &str,
    module: Option<&str>,
    health: SocketAddr,
) {
    let credentials = dir.join("credentials.json");
    std::fs::write(
        &credentials,
        format!(r#"{{"username":"bench-{index}","password":"bench-pass-{index}"}}"#,),
    )
    .unwrap();
    // Same as the integration fixture: the mock only serves map 16 ("4-4")
    // while `BotSettings.working_map` defaults to "1-8" — without the pin
    // every module would die on portal routing and skew the fleet numbers.
    let settings = match module {
        Some(module) => serde_json::json!({ "active_module": module, "working_map": "4-4" }),
        None => serde_json::json!({}),
    };
    let config = serde_json::json!({
        "schema": 1,
        "bot": { "id": format!("bot_bench_{index:02}"), "tenant": "bench" },
        "account": {
            "server": "int1.darkorbit.com",
            "auth": "password",
            "credentials": { "file": credentials }
        },
        "client": { "kind": "packet" },
        "settings": settings,
        "autostart": module.is_some(),
        "state_dir": dir.join("state"),
        "health": { "bind": health.to_string() },
        "testing": {
            "backpage_base_url": backpage,
            "game_endpoint": game
        }
    });
    std::fs::write(
        dir.join("botd.json"),
        serde_json::to_string_pretty(&config).unwrap(),
    )
    .unwrap();
}

fn spawn_bot(index: usize, exe: &Path, backpage: &str, game: &str, module: Option<&str>) -> Bot {
    let dir = tempfile::tempdir().unwrap();
    let health_addr = free_health_addr();
    write_config(dir.path(), index, backpage, game, module, health_addr);
    let log_path = dir.path().join("botd.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let mut command = Command::new(exe);
    command
        .arg("--config")
        .arg(dir.path().join("botd.json"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        // Own process group so CTRL_BREAK reaches this child alone.
        command.creation_flags(0x00000200 /* CREATE_NEW_PROCESS_GROUP */);
    }
    let child = command
        .spawn()
        .unwrap_or_else(|e| panic!("spawn bot {index}: {e}"));
    Bot {
        index,
        child,
        health_addr,
        spawned_at: Instant::now(),
        ready_at: None,
        _dir: dir,
        log_path,
        login_ms: None,
        rss_peak: 0,
        rss_sum: 0,
        cpu_sum: 0.0,
        samples: 0,
        exit: None,
        failure: None,
    }
}

/// `GET /readyz` status code; 0 on connect failure.
fn readyz(addr: SocketAddr) -> u16 {
    let Ok(mut socket) = TcpStream::connect_timeout(&addr, Duration::from_millis(400)) else {
        return 0;
    };
    let _ = socket.set_read_timeout(Some(Duration::from_millis(400)));
    if write!(
        socket,
        "GET /readyz HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
    )
    .is_err()
    {
        return 0;
    }
    let mut head = [0u8; 64];
    match socket.read(&mut head) {
        Ok(n) => String::from_utf8_lossy(&head[..n])
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0),
        Err(_) => 0,
    }
}

#[cfg(windows)]
fn request_stop(pid: u32) -> bool {
    // CTRL_BREAK_EVENT = 1; the child is its own process-group leader.
    unsafe { windows_sys::Win32::System::Console::GenerateConsoleCtrlEvent(1, pid) != 0 }
}

#[cfg(unix)]
fn request_stop(pid: u32) -> bool {
    Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

#[cfg(not(any(windows, unix)))]
fn request_stop(_pid: u32) -> bool {
    false
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn main() {
    let args = args();
    let exe = botd_exe();
    eprintln!(
        "fleet bench: {} bots × {:?} via {}",
        args.bots,
        args.duration,
        exe.display()
    );

    // One mock, N distinct accounts (ids 5000+).
    let mock = Mock::start(|config| {
        config.backpage.accounts = (0..args.bots)
            .map(|i| xenomit_mock_game::config::Account {
                username: format!("bench-{i}"),
                password: format!("bench-pass-{i}"),
                user_id: 5000 + i as i32,
                ..Default::default()
            })
            .collect();
        config.game.tick_ms = 100;
        // Keep the fleet honest: every game login must present a sid the
        // mock issued itself.
        config.game.strict_auth = true;
    });
    eprintln!("mock up: backpage {} game {}", mock.backpage, mock.game);

    // Staggered spawn: `--stagger-ms` base + deterministic jitter.
    let mut bots: Vec<Bot> = Vec::with_capacity(args.bots);
    for i in 0..args.bots {
        bots.push(spawn_bot(
            i,
            &exe,
            &mock.backpage,
            &mock.game,
            args.module.as_deref(),
        ));
        let jitter = (i as u64 * 37) % (args.stagger.as_millis() as u64 + 1);
        std::thread::sleep(args.stagger + Duration::from_millis(jitter));
    }
    let spawn_window = bots
        .last()
        .zip(bots.first())
        .map(|(last, first)| last.spawned_at.duration_since(first.spawned_at))
        .unwrap_or_default();

    // Observe + sample until the run duration ends.
    let run_end = Instant::now() + args.duration;
    let mut last_sample = Instant::now() - args.sample;
    let mut system = sysinfo::System::new();
    let login_deadline = Instant::now() + Duration::from_secs(120);
    let mut last_progress = Instant::now();
    while Instant::now() < run_end {
        // Early exits.
        for bot in &mut bots {
            if bot.exit.is_none() {
                match bot.child.try_wait() {
                    Ok(Some(status)) => {
                        bot.exit = status.code();
                        let detail = format!("exited early: {status}");
                        if bot.login_ms.is_none() && status.code() != Some(0) {
                            bot.failure.get_or_insert(detail);
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        bot.failure.get_or_insert(format!("wait error: {e}"));
                    }
                };
            }
            // Login latency: first readyz=200.
            if bot.exit.is_none() && bot.ready_at.is_none() && readyz(bot.health_addr) == 200 {
                bot.ready_at = Some(Instant::now());
                bot.login_ms = Some(bot.spawned_at.elapsed().as_millis() as u64);
            }
        }
        // Per-process RSS/CPU sample.
        if last_sample.elapsed() >= args.sample {
            last_sample = Instant::now();
            let pids: Vec<sysinfo::Pid> = bots
                .iter()
                .map(|b| sysinfo::Pid::from_u32(b.child.id()))
                .collect();
            system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&pids));
            for (bot, pid) in bots.iter_mut().zip(pids.iter()) {
                if let Some(process) = system.process(*pid) {
                    let rss = process.memory();
                    bot.rss_peak = bot.rss_peak.max(rss);
                    bot.rss_sum += rss as u128;
                    bot.cpu_sum += process.cpu_usage() as f64;
                    bot.samples += 1;
                }
            }
        }
        if last_progress.elapsed() >= Duration::from_secs(30) {
            last_progress = Instant::now();
            let ready = bots.iter().filter(|b| b.ready_at.is_some()).count();
            eprintln!(
                "  {:?}: {}/{} ready, {} game logins",
                run_end.saturating_duration_since(Instant::now()),
                ready,
                bots.len(),
                mock.shared
                    .game_logins
                    .load(std::sync::atomic::Ordering::Relaxed)
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    // Mark never-ready bots (that did not already fail).
    for bot in &mut bots {
        if bot.ready_at.is_none() && Instant::now() > login_deadline {
            bot.failure.get_or_insert("never ready within 120 s".into());
        }
    }

    // Graceful stop: CTRL_BREAK / SIGTERM → escalate to kill.
    for bot in &mut bots {
        if bot.exit.is_some() {
            continue;
        }
        let pid = bot.child.id();
        if request_stop(pid) {
            let deadline = Instant::now() + Duration::from_secs(25);
            loop {
                match bot.child.try_wait() {
                    Ok(Some(status)) => {
                        bot.exit = status.code();
                        break;
                    }
                    Ok(None) => {
                        if Instant::now() > deadline {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(100));
                    }
                    Err(e) => {
                        bot.failure.get_or_insert(format!("wait error: {e}"));
                        break;
                    }
                }
            }
        }
        if bot.exit.is_none() {
            let _ = bot.child.kill();
            if let Ok(status) = bot.child.wait() {
                bot.exit = status.code();
            }
        }
    }

    // Final session/process accounting from the mock.
    let sessions = mock.shared.sessions.lock().unwrap().len();
    let game_logins = mock
        .shared
        .game_logins
        .load(std::sync::atomic::Ordering::Relaxed);
    let game_rejects = mock
        .shared
        .game_logins_rejected
        .load(std::sync::atomic::Ordering::Relaxed);
    let http_requests = mock
        .shared
        .http_requests
        .load(std::sync::atomic::Ordering::Relaxed);
    let rx_packets: u64 = mock
        .shared
        .sessions
        .lock()
        .unwrap()
        .values()
        .map(|e| {
            e.stats
                .rx_packets
                .load(std::sync::atomic::Ordering::Relaxed)
        })
        .sum();
    let tx_packets: u64 = mock
        .shared
        .sessions
        .lock()
        .unwrap()
        .values()
        .map(|e| {
            e.stats
                .tx_packets
                .load(std::sync::atomic::Ordering::Relaxed)
        })
        .sum();

    let mock_counters = MockCounters {
        logins: game_logins,
        rejects: game_rejects,
        http_requests,
        live_sessions: sessions,
        rx_packets,
        tx_packets,
    };
    let report = build_report(&args, &bots, spawn_window, &mock_counters);
    std::fs::write(&args.report, &report).unwrap();
    eprintln!("report written to {}", args.report.display());
    print!("{report}");
}

/// Mock-side counters sampled after the fleet stops.
struct MockCounters {
    logins: u64,
    rejects: u64,
    http_requests: u64,
    live_sessions: usize,
    rx_packets: u64,
    tx_packets: u64,
}

fn build_report(args: &Args, bots: &[Bot], spawn_window: Duration, mock: &MockCounters) -> String {
    let mut out = String::new();
    let stamp = {
        // YYYY-MM-DD from system time, no chrono needed.
        let secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let days = secs / 86_400;
        let (y, m, d) = days_to_ymd(days);
        format!("{y:04}-{m:02}-{d:02}")
    };
    let profile = if botd_exe().to_string_lossy().contains("release") {
        "release"
    } else {
        "debug"
    };
    out.push_str(&format!(
        "# xenomit-botd fleet benchmark\n\n\
         Measured {stamp} on the local machine against **xenomit-mock-game** \
         (embedded mock Backpage + game server) — not the real DarkOrbit \
         services.\n\n"
    ));
    out.push_str(&format!(
        "| Setting | Value |\n|---|---|\n\
         | bots | {} |\n| duration | {:?} |\n| stagger | {:?} + jitter |\n\
         | sample interval | {:?} |\n| module | {} |\n\
         | botd binary | `{}` |\n| build profile | {profile} |\n| spawn window | {spawn_window:?} |\n\n",
        args.bots,
        args.duration,
        args.stagger,
        args.sample,
        args.module.as_deref().unwrap_or("(attach only)"),
        botd_exe().display(),
    ));

    // ---- RSS / CPU aggregates ------------------------------------------
    let mut peaks: Vec<f64> = bots
        .iter()
        .filter(|b| b.samples > 0)
        .map(|b| mib(b.rss_peak))
        .collect();
    peaks.sort_by(|a, b| a.total_cmp(b));
    let mut means: Vec<f64> = bots
        .iter()
        .filter(|b| b.samples > 0)
        .map(|b| mib((b.rss_sum / b.samples.max(1) as u128) as u64))
        .collect();
    means.sort_by(|a, b| a.total_cmp(b));
    let mut cpus: Vec<f64> = bots
        .iter()
        .filter(|b| b.samples > 0)
        .map(|b| b.cpu_sum / b.samples.max(1) as f64)
        .collect();
    cpus.sort_by(|a, b| a.total_cmp(b));
    let mut logins: Vec<f64> = bots
        .iter()
        .filter_map(|b| b.login_ms.map(|ms| ms as f64))
        .collect();
    logins.sort_by(|a, b| a.total_cmp(b));
    let fleet_cpu: f64 = cpus.iter().sum();

    out.push_str("## Per-bot resources (sysinfo)\n\n");
    out.push_str("| Metric | p50 | p95 | max |\n|---|---:|---:|---:|\n");
    out.push_str(&format!(
        "| peak RSS (MiB) | {:.1} | {:.1} | {:.1} |\n",
        percentile(&peaks, 0.50),
        percentile(&peaks, 0.95),
        peaks.last().copied().unwrap_or(0.0),
    ));
    out.push_str(&format!(
        "| mean RSS (MiB) | {:.1} | {:.1} | {:.1} |\n",
        percentile(&means, 0.50),
        percentile(&means, 0.95),
        means.last().copied().unwrap_or(0.0),
    ));
    out.push_str(&format!(
        "| mean CPU (% of one core) | {:.2} | {:.2} | {:.2} |\n",
        percentile(&cpus, 0.50),
        percentile(&cpus, 0.95),
        cpus.last().copied().unwrap_or(0.0),
    ));
    out.push_str(&format!(
        "| login time spawn→ready (ms) | {:.0} | {:.0} | {:.0} |\n",
        percentile(&logins, 0.50),
        percentile(&logins, 0.95),
        logins.last().copied().unwrap_or(0.0),
    ));
    // sysinfo `cpu_usage` is per-core (100% = one core saturated).
    let cores = std::thread::available_parallelism()
        .map(|n| n.get() as f64)
        .unwrap_or(1.0);
    out.push_str(&format!(
        "\nFleet mean CPU total: **{fleet_cpu:.1}%** of one core = \
         **{:.1}%** of machine capacity ({cores:.0} logical cores, \
         sysinfo `cpu_usage` scale).\n\n",
        fleet_cpu / cores
    ));

    // ---- Mock-side counters ---------------------------------------------
    out.push_str("## Mock-side activity\n\n");
    out.push_str("| Counter | Value |\n|---|---:|\n");
    out.push_str(&format!(
        "| backpage HTTP requests | {} |\n",
        mock.http_requests
    ));
    out.push_str(&format!("| game logins accepted | {} |\n", mock.logins));
    out.push_str(&format!("| game logins rejected | {} |\n", mock.rejects));
    out.push_str(&format!(
        "| live sessions at stop | {} |\n",
        mock.live_sessions
    ));
    out.push_str(&format!(
        "| packets received (live sessions) | {} |\n",
        mock.rx_packets
    ));
    out.push_str(&format!(
        "| packets sent (live sessions) | {} |\n\n",
        mock.tx_packets
    ));

    // ---- Failures --------------------------------------------------------
    let ready = bots.iter().filter(|b| b.ready_at.is_some()).count();
    let clean_exits = bots.iter().filter(|b| matches!(b.exit, Some(0))).count();
    let failures: Vec<&Bot> = bots.iter().filter(|b| b.failure.is_some()).collect();
    out.push_str("## Outcome\n\n");
    out.push_str("| Result | Count |\n|---|---:|\n");
    out.push_str(&format!(
        "| reached ready (/readyz 200) | {ready} / {} |\n",
        bots.len()
    ));
    out.push_str(&format!(
        "| clean exit (code 0) | {clean_exits} / {} |\n",
        bots.len()
    ));
    out.push_str(&format!("| failed | {} |\n\n", failures.len()));
    if !failures.is_empty() {
        out.push_str("| bot | failure | log |\n|---|---|---|\n");
        for bot in &failures {
            out.push_str(&format!(
                "| {} | {} | `{}` |\n",
                bot.index,
                bot.failure.as_deref().unwrap_or("?"),
                bot.log_path.display()
            ));
        }
        out.push('\n');
    }
    out.push_str(
        "Notes: per-tick durations are not yet exposed by botd (no module \
         timing metric) — activity is evidenced by the packet counters above. \
         The mock adds no network latency and runs on localhost, so real-ISP \
         numbers will be higher; treat this as a lower bound for sizing.\n",
    );
    out
}

/// Civil date from days since epoch (Howard Hinnant's algorithm).
fn days_to_ymd(days: u64) -> (i64, u64, u64) {
    let z = days as i64 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}
