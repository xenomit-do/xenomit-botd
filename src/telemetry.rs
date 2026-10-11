//! Telemetry wiring (orchestrator `docs/06-observability.md`, phase 2).
//!
//! [`logging_config`] maps `botd.json`'s `telemetry` block plus `OTEL_*`
//! environment onto `xenomit-core`'s [`logging::Config`], stamping the
//! bot's identity onto every exported signal as resource attributes.
//! [`BotMetrics`] holds the daemon's instruments (docs/06 §6.6) — plain
//! no-ops when no collector is configured.
//!
//! `bot.id`/`tenant` ride as *resource* attributes, never metric labels
//! (docs/06 §6.3 cardinality rule).

use std::time::Duration;

use xenomit_core::logging;
use xenomit_core::logging::opentelemetry::KeyValue;
use xenomit_core::logging::opentelemetry::metrics::{Counter, Gauge, Histogram};

use crate::config::BotdConfig;
use crate::state::Phase;

/// `service.instance.id`: pod name in k8s, the machine hostname elsewhere.
fn instance_id() -> Option<String> {
    std::env::var("POD_NAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .ok()
        .filter(|value| !value.trim().is_empty())
}

/// `xenomit-core` logging config for this bot. `telemetry.otlp_endpoint`
/// wins; absent it falls back to `OTEL_EXPORTER_OTLP_ENDPOINT` (and the
/// signal-specific `OTEL_EXPORTER_OTLP_*_ENDPOINT`s), unset disables
/// export entirely. `OTEL_RESOURCE_ATTRIBUTES`/`OTEL_SERVICE_NAME` are
/// honored by the SDK's env resource detector.
pub fn logging_config(config: &BotdConfig) -> logging::Config {
    let otlp = match &config.telemetry.otlp_endpoint {
        Some(endpoint) => logging::Otlp::Enabled {
            endpoint: Some(endpoint.clone()),
        },
        None => logging::Otlp::Auto,
    };
    let mut logging = logging::Config::new("xenomit-botd")
        .with_service_version(env!("CARGO_PKG_VERSION"))
        .with_otlp(otlp)
        .with_resource_attr("xenomit.bot.id", config.bot.id.clone())
        .with_resource_attr("xenomit.tenant", config.bot.tenant.clone());
    if let Some(instance) = instance_id() {
        logging = logging.with_resource_attr("service.instance.id", instance);
    }
    logging
}

/// The daemon's instruments (docs/06 §6.6). Construct after
/// [`logging::init_with`]: instruments bind to the meter provider that is
/// global at construction time, and stay inert no-ops without one.
#[derive(Clone)]
pub struct BotMetrics {
    login_duration: Histogram<f64>,
    tick_duration: Histogram<f64>,
    phase: Gauge<u64>,
    stats_events: Counter<u64>,
    restarts: Counter<u64>,
    version: &'static str,
}

impl BotMetrics {
    pub fn new() -> Self {
        Self {
            login_duration: logging::metrics::histogram(
                "xenomit.login.duration",
                "Backpage authentication duration",
                "s",
            ),
            tick_duration: logging::metrics::histogram(
                "xenomit.runtime.tick_duration",
                "Supervisor loop iteration duration",
                "s",
            ),
            phase: logging::metrics::gauge("xenomit.bot.phase", "Bot lifecycle phase"),
            stats_events: logging::metrics::counter(
                "xenomit.stats.events",
                "Statistics events journaled to sinks",
            ),
            restarts: logging::metrics::counter("xenomit.bot.restarts", "Daemon process starts"),
            version: env!("CARGO_PKG_VERSION"),
        }
    }

    /// Backpage authenticate latency; `outcome` ∈ `ok`/`failed`.
    pub fn record_login(&self, elapsed: Duration, outcome: &'static str) {
        self.login_duration
            .record(elapsed.as_secs_f64(), &[KeyValue::new("outcome", outcome)]);
    }

    /// One supervisor iteration; `module` is the running module or `idle`.
    pub fn record_tick(&self, module: &str, elapsed: Duration) {
        self.tick_duration.record(
            elapsed.as_secs_f64(),
            &[KeyValue::new("module", module.to_owned())],
        );
    }

    /// Phase transitions, labeled per docs/06 §6.6 (`phase`, `version`).
    pub fn set_phase(&self, phase: Phase) {
        self.phase.record(
            1,
            &[
                KeyValue::new("phase", phase.to_string()),
                KeyValue::new("version", self.version),
            ],
        );
    }

    /// One journaled statistics event.
    pub fn stats_event(&self) {
        self.stats_events.add(1, &[]);
    }

    /// One daemon process start — summed over time, this is the restart
    /// count of the bot's pods.
    pub fn record_start(&self) {
        self.restarts.add(1, &[]);
    }
}

impl Default for BotMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_config() -> BotdConfig {
        serde_json::from_value(json!({
            "schema": 1,
            "bot": {"id": "bot_t", "tenant": "t"},
            "account": {
                "server": "int1.darkorbit.com",
                "auth": "session",
                "credentials": {"file": "unused"}
            },
            "client": {"kind": "packet"},
            "telemetry": {"otlp_endpoint": "http://localhost:4317"}
        }))
        .unwrap()
    }

    #[test]
    fn logging_config_maps_telemetry_block() {
        let rendered = format!("{:?}", logging_config(&test_config()));
        assert!(rendered.contains("xenomit.bot.id"), "{rendered}");
        assert!(rendered.contains("bot_t"), "{rendered}");
        assert!(rendered.contains("xenomit.tenant"), "{rendered}");
        assert!(rendered.contains("4317"), "{rendered}");

        // No telemetry block → Otlp::Auto (OTEL_* env decides).
        let config: BotdConfig = serde_json::from_value(json!({
            "schema": 1,
            "bot": {"id": "bot_t", "tenant": "t"},
            "account": {
                "server": "int1.darkorbit.com",
                "auth": "session",
                "credentials": {"file": "unused"}
            },
            "client": {"kind": "packet"}
        }))
        .unwrap();
        let rendered = format!("{:?}", logging_config(&config));
        assert!(rendered.contains("Auto"), "{rendered}");
    }

    /// docs/06 §6.2 / phase-2 exit criterion: a secret logged inside botd
    /// must never reach the exporter. Drives the same pipeline
    /// `init_with` builds — `tracing` event → redacting exporter — with
    /// the wire replaced by in-memory exporters (dev-dependency feature
    /// `xenomit_core/testing`).
    #[test]
    fn secret_logged_in_botd_never_reaches_exporter() {
        let capture = logging::testing::capture();

        logging::tracing::warn!(
            dosid = "s3cr3t-dosid",
            password = "hunter2",
            "login failed for dosid=s3cr3t-dosid at https://int1.darkorbit.com/?sid=s3cr3t-dosid"
        );
        logging::tracing::info_span!("bot.login", session_id = "s3cr3t-dosid")
            .in_scope(|| logging::tracing::info!("attach attempt"));

        let exported = format!("{:?}", capture.emitted_logs());
        assert!(exported.contains("<redacted>"), "{exported}");
        assert!(!exported.contains("s3cr3t-dosid"), "{exported}");
        assert!(!exported.contains("hunter2"), "{exported}");

        let spans = format!("{:?}", capture.finished_spans());
        assert!(!spans.contains("s3cr3t-dosid"), "{spans}");
    }
}
