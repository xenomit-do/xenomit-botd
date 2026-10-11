//! Statistics wiring. The durable outbox lives in `Statistics` itself
//! (`xenomit-bot-runtime`): the JSONL journal under `state_dir/statistics`
//! is replayed on restart, events with `seq > cursor` stay unacked, and the
//! ack cursor persists to a `.cursor` sidecar (docs/03 §3.6). This module
//! only adds the telemetry counter sink.
use xenomit_bot_runtime::statistics::{Event, StatSink};

use crate::telemetry::BotMetrics;

/// Counts every appended event on `xenomit.stats.events` (docs/06 §6.6).
pub struct MetricsSink {
    metrics: BotMetrics,
}

impl MetricsSink {
    pub fn new(metrics: BotMetrics) -> Self {
        Self { metrics }
    }
}

impl StatSink for MetricsSink {
    fn record(&self, _event: &Event) -> Result<(), String> {
        self.metrics.stats_event();
        Ok(())
    }
}
