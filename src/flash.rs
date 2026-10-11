//! Owned Linux Pepper process. The final client reference terminates its process group.
use crate::daemon::GameConnector;
use crate::error::BotdError;
use std::io::Write;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use xenomit_backpage::Session;
use xenomit_bot_runtime::SharedStats;
use xenomit_game_api::{ClientStatus, GameClient, Result as ApiResult};

pub struct FlashConnector;

struct OwnedProcess(Child);
impl Drop for OwnedProcess {
    fn drop(&mut self) {
        #[cfg(target_os = "linux")]
        // SAFETY: spawn creates a private process group whose id is the child's pid.
        unsafe {
            libc::kill(-(self.0.id() as i32), libc::SIGKILL);
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
        for path in [
            format!("/dev/shm/xenomit_state_{}", self.0.id()),
            format!("/tmp/xenomit_state_{}", self.0.id()),
            format!("/dev/shm/xenomit_state_{}.navigation", self.0.id()),
            format!("/dev/shm/xenomit_state_{}.navigation.tmp", self.0.id()),
            format!("/tmp/xenomit_state_{}.navigation", self.0.id()),
            format!("/tmp/xenomit_state_{}.navigation.tmp", self.0.id()),
            format!("/tmp/xenomit_flash_{}.sock", self.0.id()),
            format!("/tmp/xenomit-flash-{}.html", self.0.id()),
        ] {
            let _ = std::fs::remove_file(path);
        }
    }
}

fn bootstrap(session: &Session) -> Result<String, BotdError> {
    let movie = session
        .preloader_url
        .as_deref()
        .ok_or_else(|| BotdError::Config("Flash preloader URL missing".into()))?;
    let mut vars = session.flashvars.clone();
    vars.insert("autoStartEnabled".into(), "1".into());
    vars.insert("display2d".into(), "2".into());
    let mut pairs: Vec<_> = vars.iter().collect();
    pairs.sort_by(|a, b| a.0.cmp(b.0));
    let vars = pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let instance = session.instance();
    if [movie, &instance, &session.sid, &vars]
        .iter()
        .any(|s| s.contains(['\n', '\r', '\0']) || s.len() > 65535)
    {
        return Err(BotdError::Config(
            "Flash bootstrap contains an invalid record".into(),
        ));
    }
    Ok(format!(
        "{movie}\n{instance}\n{}\n{vars}\n{}\n{}\n",
        session.sid,
        if session.stage_width > 100 {
            session.stage_width
        } else {
            1280
        },
        if session.stage_height > 100 {
            session.stage_height
        } else {
            800
        }
    ))
}

impl GameConnector for FlashConnector {
    fn connect(
        &self,
        session: &Session,
        _statistics: SharedStats,
        game_endpoint: Option<&str>,
    ) -> Result<Arc<dyn GameClient>, BotdError> {
        if !cfg!(target_os = "linux") || game_endpoint.is_some() {
            return Err(BotdError::Config(
                "Flash requires Linux and does not support testing.game_endpoint".into(),
            ));
        }
        let input = bootstrap(session)?;
        let executable = std::env::var_os("XENOMIT_FLASH_CLIENT_EXE")
            .unwrap_or_else(|| "/opt/xenomit-flash-client/xenomit_flash_client".into());
        let mut command = Command::new(executable);
        command
            .args(["--launch-stdin", "--backend", "native", "--parent-pid"])
            .arg(std::process::id().to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
        // Keep the player mapped on Xvfb so Flash/Flex continues drawing frames.
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        let mut process = OwnedProcess(
            command
                .spawn()
                .map_err(|e| BotdError::Config(format!("Could not launch Flash: {e}")))?,
        );
        process
            .0
            .stdin
            .take()
            .unwrap()
            .write_all(input.as_bytes())
            .map_err(|e| BotdError::Unavailable(format!("Could not send Flash bootstrap: {e}")))?;
        let inner = xenomit_game_api::connect_flash_native(process.0.id())
            .map_err(|e| BotdError::Unavailable(e.to_string()))?;
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            if let Some(status) = process
                .0
                .try_wait()
                .map_err(|e| BotdError::Unavailable(e.to_string()))?
            {
                return Err(BotdError::Unavailable(format!(
                    "Flash exited during startup: {status}"
                )));
            }
            if inner.status().is_ok_and(|s| s.ready) && inner.ping().is_ok() {
                break;
            }
            if Instant::now() >= deadline {
                return Err(BotdError::Unavailable(
                    "Flash did not become ready with a live frame and command bridge within 120 seconds".into(),
                ));
            }
            std::thread::sleep(Duration::from_millis(150));
        }
        Ok(Arc::new(FlashProcessClient {
            inner,
            process: Mutex::new(process),
        }))
    }
}

struct FlashProcessClient {
    inner: Arc<dyn GameClient>,
    process: Mutex<OwnedProcess>,
}

use xenomit_game_api::{ActionResult, FunctionCall, GameSnapshot, Inspection};
impl GameClient for FlashProcessClient {
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
        if self
            .process
            .lock()
            .map_err(|_| {
                xenomit_game_api::ApiError::Unavailable("Flash process lock poisoned".into())
            })?
            .0
            .try_wait()
            .map_err(|e| xenomit_game_api::ApiError::Unavailable(e.to_string()))?
            .is_some()
        {
            return Err(xenomit_game_api::ApiError::Unavailable(
                "Flash process exited".into(),
            ));
        }
        self.inner.status()
    }

    fn ping(&self) -> ApiResult<()> {
        self.inner.ping()
    }

    fn snapshot(&self, target: Option<&str>) -> ApiResult<GameSnapshot> {
        let snapshot = self.inner.snapshot(target);
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
    #[test]
    fn bootstrap_rejects_record_injection_and_missing_movie() {
        let mut session = Session::new("sid", "int1");
        assert!(bootstrap(&session).is_err());
        session.preloader_url = Some("https://example.test/movie.swf".into());
        let input = bootstrap(&session).unwrap();
        assert_eq!(input.lines().count(), 6);
        assert!(input.ends_with("1280\n800\n"));
        session.sid = "sid\nextra".into();
        assert!(bootstrap(&session).is_err());
    }
    #[cfg(target_os = "linux")]
    #[test]
    fn owned_process_terminates_and_reaps_group() {
        use std::os::unix::process::CommandExt;
        let child = Command::new("sleep")
            .arg("60")
            .process_group(0)
            .spawn()
            .unwrap();
        let pid = child.id();
        drop(OwnedProcess(child));
        // SAFETY: signal 0 only probes the now reaped test process.
        assert_eq!(unsafe { libc::kill(pid as i32, 0) }, -1);
    }
}
