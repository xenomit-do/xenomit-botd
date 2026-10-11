//! Command line: `--config <path>`, `--print-schema`, `--help`, `--version`.
use std::path::PathBuf;

use xenomit_core::logging;

use crate::config::{self, BotdConfig};
use crate::daemon::Daemon;
use crate::error::ExitCode;

const USAGE: &str = "\
xenomit-botd — headless Xenomit bot daemon

USAGE:
    xenomit-botd [--config <path>]
    xenomit-botd --print-schema
    xenomit-botd --help | --version

CONFIG PATH PRECEDENCE:
    1. --config <path>
    2. $XENOMIT_BOTD_CONFIG
    3. /etc/xenomit/botd.json
";

enum Action {
    Run { config: Option<PathBuf> },
    PrintSchema,
    Help,
    Version,
}

fn parse(args: &[String]) -> Result<Action, String> {
    let mut config = None;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--print-schema" => return Ok(Action::PrintSchema),
            "-h" | "--help" => return Ok(Action::Help),
            "--version" | "-V" => return Ok(Action::Version),
            "--config" => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| "--config requires a path".to_owned())?;
                config = Some(PathBuf::from(value));
            }
            other if other.starts_with("--config=") => {
                config = Some(PathBuf::from(&other["--config=".len()..]));
            }
            other => return Err(format!("Unknown argument {other}")),
        }
        i += 1;
    }
    Ok(Action::Run { config })
}

/// Full entry point; returns the process exit code.
pub fn run(args: impl Iterator<Item = String>) -> i32 {
    let args: Vec<String> = args.collect();
    let action = match parse(&args) {
        Ok(action) => action,
        Err(error) => {
            eprintln!("{error}\n\n{USAGE}");
            return ExitCode::InvalidConfig.code();
        }
    };
    match action {
        Action::Help => {
            println!("{USAGE}");
            ExitCode::Clean.code()
        }
        Action::Version => {
            println!("xenomit-botd {}", env!("CARGO_PKG_VERSION"));
            ExitCode::Clean.code()
        }
        Action::PrintSchema => {
            println!("{}", config::json_schema());
            ExitCode::Clean.code()
        }
        Action::Run { config } => {
            let path = config::resolve_path(config);
            let config = match BotdConfig::load(&path) {
                Ok(config) => config,
                Err(error) => {
                    eprintln!("{error}");
                    return ExitCode::InvalidConfig.code();
                }
            };
            let guard = init_logging(&config);
            let code = Daemon::production(config).run();
            drop(guard);
            code.code()
        }
    }
}

fn init_logging(config: &BotdConfig) -> Option<logging::Guard> {
    match logging::init_with(crate::telemetry::logging_config(config)) {
        Ok(guard) => Some(guard),
        Err(error) => {
            eprintln!("Could not initialize logging: {error}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_flags() {
        assert!(matches!(
            parse(&["--print-schema".into()]).unwrap(),
            Action::PrintSchema
        ));
        assert!(matches!(parse(&[]).unwrap(), Action::Run { config: None }));
        match parse(&["--config".into(), "/tmp/botd.json".into()]).unwrap() {
            Action::Run { config } => {
                assert_eq!(config, Some(PathBuf::from("/tmp/botd.json")));
            }
            _ => panic!("expected Run"),
        }
        match parse(&["--config=/x.json".into()]).unwrap() {
            Action::Run { config } => assert_eq!(config, Some(PathBuf::from("/x.json"))),
            _ => panic!("expected Run"),
        }
        assert!(parse(&["--bogus".into()]).is_err());
        assert!(parse(&["--config".into()]).is_err());
    }
}
