//! `xenomit-botd` — the headless Xenomit bot daemon
//! (orchestrator `docs/02-headless-bot.md`, phase 1a).
//!
//! The daemon embeds [`xenomit_bot_runtime`] instead of forking the GPUI
//! app. It resolves `--config`/`XENOMIT_BOTD_CONFIG`/`/etc/xenomit/botd.json`
//! (schema v1), loads credentials through [`secrets::SecretResolver`],
//! authenticates via Backpage, attaches the packet game client, runs
//! `PlannerRuntime` with headless front-end implementations, optionally
//! links the web panel (Agent Protocol v1), and serves `/startupz`,
//! `/readyz`, `/livez` on localhost.
//!
//! Phases: `Boot → LoadConfig → Arm → Activate → Login → Attach → Run`.

pub mod cli;
pub mod config;
pub mod control;
pub mod daemon;
pub mod error;
pub mod flash;
pub mod health;
pub mod heartbeat;
pub mod host;
pub mod panel;
pub mod proxy;
pub mod secrets;
pub mod session;
pub mod settings;
pub mod snapshot;
pub mod state;
pub mod stats;
pub mod stream;
pub mod telemetry;
