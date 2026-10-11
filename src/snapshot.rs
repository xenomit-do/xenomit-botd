//! Snapshot assembly and the local file [`SnapshotStore`] (docs/03 §3.3,
//! §3.8; `SnapshotStore` trait from `fleet-types`, ADR-22).
//!
//! ## Payload mapping
//!
//! The daemon writes `BotSnapshot` documents. The authoritative runtime
//! state is the serialized [`RuntimeCheckpoint`] stored verbatim under
//! `module.state` — it carries the engine `ModuleState` envelope (schema,
//! settings fingerprint, degradation flags, issues), the rule blobs and the
//! worker clock offset in one self-contained, versioned blob. The typed
//! `module.id`/`module.state_version`/`module.sequence`/`behaviors`/
//! `stats`/`game_hint` sections are populated as observability mirrors for
//! readers that do not know the envelope (docs/03 §3.3 shape).
//!
//! ## Local store layout
//!
//! ```text
//! <root>/epochs/<bot>.txt            current fencing epoch ("7")
//! <root>/snapshots/<id>.json         StoredSnapshot (meta + document)
//! ```
//!
//! All writes go through `write tmp + rename` so a crash never leaves a
//! half-written record, and `put` is epoch-conditional exactly like the
//! Postgres implementation (`WHERE epoch = current_epoch`, docs/03 §3.7).
//! Two `botd` processes sharing a `state_dir` fence each other through the
//! epoch file, which is what the kill-matrix tests exercise.
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde_json::Value;

use fleet_types::epoch::{Epoch, Fenced};
use fleet_types::snapshot::SNAPSHOT_SCHEMA;
use fleet_types::snapshot::{
    BehaviorSection, BotSnapshot, CHECKPOINT_OUTBOX_TAIL, GameHint, ModuleSection, SequenceState,
    SnapshotId, SnapshotMeta, SnapshotQuality, SnapshotReason, SnapshotStore, StatsSection,
    StoreError, StoredSnapshot, WriterInfo,
};
use xenomit_bot_runtime::handoff::{
    CheckpointQuality, PendingRestore, RestorePolicy, RuntimeCheckpoint,
};
use xenomit_bot_runtime::statistics::{Event, Statistics};

/// Oldest botd that can read this snapshot — the rollback gate (docs/03
/// §3.5 `min_reader`). This is the first snapshot-capable release.
const MIN_READER: &str = env!("CARGO_PKG_VERSION");

/// `SystemTime` now in unix milliseconds.
pub fn unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Drive an async [`SnapshotStore`] call from the synchronous supervisor.
/// Store implementations are in-process (file/memory) so a scratch
/// current-thread runtime suffices.
pub fn block_on<F: std::future::Future>(future: F) -> F::Output {
    match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime.block_on(future),
        // No runtime available — the future is synchronous work behind an
        // async signature; poll it inline via a minimal block-on.
        Err(_) => futures_block_on(future),
    }
}

/// Last-resort block_on: spin-polls a future that never actually pends
/// (all store methods complete synchronously).
fn futures_block_on<F: std::future::Future>(future: F) -> F::Output {
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
    fn noop(_: *const ()) {}
    fn clone(_: *const ()) -> RawWaker {
        RawWaker::new(std::ptr::null(), &VTABLE)
    }
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
    let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) };
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

// ---------------------------------------------------------------------
// Assembly: RuntimeCheckpoint + Statistics -> BotSnapshot
// ---------------------------------------------------------------------

/// Map a checkpoint fidelity onto the wire `quality` field.
pub fn wire_quality(quality: CheckpointQuality) -> SnapshotQuality {
    match quality {
        CheckpointQuality::Clean => SnapshotQuality::Clean,
        CheckpointQuality::Degraded => SnapshotQuality::Degraded,
        CheckpointQuality::Checkpoint => SnapshotQuality::Checkpoint,
    }
}

/// Map a drain reason string onto the snapshot `reason` vocabulary
/// (`fleet_types::HandoffReason` strings, docs/03 §3.2).
pub fn wire_reason(reason: &str) -> SnapshotReason {
    match reason {
        "upgrade" => SnapshotReason::Upgrade,
        "eviction" => SnapshotReason::Eviction,
        "proxy_change" => SnapshotReason::ProxyChange,
        "checkpoint" => SnapshotReason::Checkpoint,
        "crash_recovered" => SnapshotReason::CrashRecovered,
        "desired_state" | "pause" | "stop" => SnapshotReason::DesiredState,
        _ => SnapshotReason::Manual,
    }
}

/// The `stats` section: derived counters + the outbox cursor and unacked
/// tail (docs/03 §3.6). Events themselves are never embedded beyond the
/// bounded tail.
fn stats_section(statistics: Option<&Statistics>) -> StatsSection {
    let mut section = StatsSection::default();
    if let Some(stats) = statistics {
        section.session_started_unix_ms = stats.started as i64;
        section.balances_last = stats.balances.map(|b| b.unwrap_or(0.0)).to_vec();
        section.rate_window = rate_window(stats);
        section.outbox_cursor = stats.outbox_cursor();
        section.outbox_tail = stats
            .outbox_tail()
            .take(CHECKPOINT_OUTBOX_TAIL)
            .filter_map(|event| serde_json::to_value(event).ok())
            .collect();
    }
    section
}

/// Per-minute gross rewards over the retained window, compacted (docs/03
/// §3.3 `rate_window`). Only minutes with nonzero rewards are emitted.
fn rate_window(stats: &Statistics) -> Vec<Value> {
    let mut buckets: BTreeMap<u64, [f64; 4]> = BTreeMap::new();
    for event in &stats.events {
        if event.rewards.iter().all(|v| *v == 0.0) {
            continue;
        }
        let minute = event.at / 60_000 * 60_000;
        let bucket = buckets.entry(minute).or_default();
        for (i, value) in event.rewards.iter().enumerate() {
            bucket[i] += value;
        }
    }
    buckets
        .into_iter()
        .map(|(minute, rewards)| serde_json::json!({ "t": minute, "rewards": rewards }))
        .collect()
}

/// Mirror of `module.sequence` for observability (docs/03 §3.3). Filled
/// when the module blob is a sequence checkpoint; otherwise absent.
fn sequence_mirror(checkpoint: &RuntimeCheckpoint) -> Option<SequenceState> {
    let data = &checkpoint.module.as_ref()?.data;
    // A TaskSequence checkpoint is identified by its required fields.
    let run_key = data.get("run_key")?.as_str()?.to_owned();
    Some(SequenceState {
        run_key: Some(run_key),
        step: data
            .get("current_step")
            .and_then(Value::as_str)
            .map(str::to_owned),
        attempt: data.get("attempt").and_then(Value::as_u64).unwrap_or(0) as u32,
        results: data.get("results").cloned().unwrap_or(Value::Null),
        elapsed_in_step_ms: data
            .get("entered_elapsed_ms")
            .and_then(Value::as_u64)
            .unwrap_or(0),
    })
}

/// Mirror of the `behaviors` section: rule blobs keyed by rule id.
fn behaviors_mirror(checkpoint: &RuntimeCheckpoint) -> BTreeMap<String, BehaviorSection> {
    checkpoint
        .rules
        .as_object()
        .map(|rules| {
            rules
                .iter()
                .map(|(id, state)| {
                    (
                        id.clone(),
                        BehaviorSection {
                            state_version: 1,
                            state: state.clone(),
                        },
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Build the wire snapshot for a captured [`RuntimeCheckpoint`]. A `None`
/// checkpoint produces a stats-only document (worker never answered the
/// drain — degraded by construction).
#[allow(clippy::too_many_arguments)]
pub fn assemble(
    bot_id: &str,
    epoch: Epoch,
    reason: SnapshotReason,
    quality: SnapshotQuality,
    checkpoint: Option<RuntimeCheckpoint>,
    statistics: Option<&Statistics>,
    settings_rev: u64,
    run_state: &str,
    active_module: &str,
) -> BotSnapshot {
    let mut snapshot = BotSnapshot::new(bot_id, epoch, reason, quality, unix_ms());
    snapshot.seq = statistics.map(|s| s.next_seq()).unwrap_or(0);
    snapshot.writer = WriterInfo {
        botd: env!("CARGO_PKG_VERSION").to_owned(),
        engine: xenomit_behavior_engine::VERSION.to_owned(),
        packet_schema: xenomit_packet_client::modules::VERSION_HASH.to_owned(),
    };
    snapshot.min_reader = MIN_READER.to_owned();
    snapshot.runtime.active_module = checkpoint
        .as_ref()
        .and_then(|cp| cp.module_id.clone())
        .or_else(|| (!active_module.is_empty()).then(|| active_module.to_owned()));
    snapshot.runtime.settings_rev = settings_rev;
    snapshot.runtime.run_state = run_state.to_owned();
    if let Some(checkpoint) = checkpoint {
        let state_version = checkpoint.module.as_ref().map(|m| m.schema).unwrap_or(1);
        let sequence = sequence_mirror(&checkpoint);
        snapshot.behaviors = behaviors_mirror(&checkpoint);
        snapshot.game_hint = checkpoint.game_hint.as_ref().map(|hint| GameHint {
            map_id: hint.get("map_id").and_then(Value::as_i64),
            hero_x: hint.get("hero_x").and_then(Value::as_i64),
            hero_y: hint.get("hero_y").and_then(Value::as_i64),
        });
        snapshot.module = checkpoint.module_id.clone().map(|id| ModuleSection {
            id,
            state_version,
            // The authoritative runtime payload: the serialized
            // RuntimeCheckpoint envelope (module state, rules, clock
            // offset, issues). `module.sequence`/`behaviors` are mirrors.
            state: serde_json::to_value(&checkpoint).unwrap_or(Value::Null),
            sequence,
        });
    }
    snapshot.stats = stats_section(statistics);
    // The deaths mirror reads the assembled `behaviors`/`module` sections.
    snapshot.stats.deaths = checkpoint_deaths(&snapshot);
    snapshot
}

/// `stats.deaths` after assembly: rule blobs live under `behaviors` once
/// mirrored, plus the module envelope's own counter.
fn checkpoint_deaths(snapshot: &BotSnapshot) -> u64 {
    let rules_deaths: u64 = snapshot
        .behaviors
        .values()
        .filter_map(|section| section.state.get("death_count").and_then(Value::as_u64))
        .sum();
    let module_deaths = snapshot
        .module
        .as_ref()
        .and_then(|m| m.state.get("module"))
        .and_then(|m| m.get("data"))
        .and_then(|d| d.get("death_count"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    rules_deaths + module_deaths
}

// ---------------------------------------------------------------------
// Restore: StoredSnapshot -> PendingRestore + stats replay plan
// ---------------------------------------------------------------------

/// What a snapshot contributes to a restore: the runtime checkpoint plus
/// the event-stream cursor/tail for `Statistics::restore_outbox`
/// (docs/03 §3.6 — replays are idempotent through `(instanceId, seq)`).
pub struct RestorePlan {
    pub pending: PendingRestore,
    pub outbox_cursor: u64,
    pub outbox_tail: Vec<Event>,
    /// `crash_recovered` snapshots flag the restore provenance in logs.
    pub reason: SnapshotReason,
}

/// Decode a stored snapshot into a restore plan. `module.state` is the
/// authoritative envelope; when a writer stored a bare `ModuleState`
/// instead (forward compatibility) the `behaviors` section supplies rules.
pub fn restore_plan(stored: &StoredSnapshot, policy: RestorePolicy) -> Result<RestorePlan, String> {
    let snapshot = &stored.snapshot;
    if snapshot.schema > SNAPSHOT_SCHEMA {
        return Err(format!(
            "snapshot schema {} is newer than supported {}",
            snapshot.schema, SNAPSHOT_SCHEMA
        ));
    }
    let checkpoint = match &snapshot.module {
        Some(section) => restore_checkpoint(section, &snapshot.behaviors)?,
        None => RuntimeCheckpoint {
            schema: RuntimeCheckpoint::SCHEMA,
            module_id: None,
            module: None,
            rules: Value::Null,
            run_elapsed_ms: 0,
            game_hint: None,
            issues: Vec::new(),
        },
    };
    let quality = match snapshot.quality {
        SnapshotQuality::Clean => CheckpointQuality::Clean,
        SnapshotQuality::Degraded => CheckpointQuality::Degraded,
        SnapshotQuality::Checkpoint => CheckpointQuality::Checkpoint,
    };
    let outbox_tail = snapshot
        .stats
        .outbox_tail
        .iter()
        .filter_map(|event| serde_json::from_value::<Event>(event.clone()).ok())
        .collect();
    Ok(RestorePlan {
        pending: PendingRestore {
            snapshot_id: Some(stored.meta.id.0.clone()),
            checkpoint,
            quality,
            trace_id: None,
            policy,
        },
        outbox_cursor: snapshot.stats.outbox_cursor,
        outbox_tail,
        reason: snapshot.reason,
    })
}

/// `module.state` → [`RuntimeCheckpoint`]. The daemon writes the envelope
/// verbatim; a bare `ModuleState` blob (another writer) is wrapped with the
/// `behaviors` section as the rule source.
fn restore_checkpoint(
    section: &ModuleSection,
    behaviors: &BTreeMap<String, BehaviorSection>,
) -> Result<RuntimeCheckpoint, String> {
    if section.state.get("module_id").is_some() || section.state.get("module").is_some() {
        return serde_json::from_value::<RuntimeCheckpoint>(section.state.clone())
            .map_err(|error| format!("runtime checkpoint corrupt: {error}"));
    }
    // Bare ModuleState + typed behaviors sections.
    let module: xenomit_behavior_engine::ModuleState =
        serde_json::from_value(section.state.clone())
            .map_err(|error| format!("module state corrupt: {error}"))?;
    let rules = Value::Object(
        behaviors
            .iter()
            .map(|(id, section)| (id.clone(), section.state.clone()))
            .collect(),
    );
    Ok(RuntimeCheckpoint {
        schema: RuntimeCheckpoint::SCHEMA,
        module_id: Some(section.id.clone()),
        module: Some(module),
        rules,
        run_elapsed_ms: 0,
        game_hint: None,
        issues: Vec::new(),
    })
}

// ---------------------------------------------------------------------
// LocalSnapshotStore — the standalone/dev SnapshotStore over files
// ---------------------------------------------------------------------

/// `state_dir/snapshots` file store. Fencing: `<bot>.txt` under `epochs/`
/// records the current epoch; `put` refuses any other epoch
/// (`StoreError::Fenced` → the daemon exits 67).
pub struct LocalSnapshotStore {
    root: PathBuf,
    /// In-process serializer for read-modify-write of the epoch file and
    /// snapshot id allocation. Cross-process safety rides on atomic
    /// renames + epoch fencing.
    lock: Mutex<()>,
    seq: AtomicU64,
}

impl LocalSnapshotStore {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            lock: Mutex::new(()),
            seq: AtomicU64::new(0),
        }
    }

    fn epochs_dir(&self) -> PathBuf {
        self.root.join("epochs")
    }
    fn snapshots_dir(&self) -> PathBuf {
        self.root.join("snapshots")
    }
    fn epoch_path(&self, bot_id: &str) -> PathBuf {
        self.epochs_dir().join(format!("{}.txt", sanitize(bot_id)))
    }
    fn snapshot_path(&self, id: &SnapshotId) -> PathBuf {
        self.snapshots_dir()
            .join(format!("{}.json", sanitize(&id.0)))
    }

    fn read_epoch(&self, bot_id: &str) -> Epoch {
        std::fs::read_to_string(self.epoch_path(bot_id))
            .ok()
            .and_then(|body| body.trim().parse::<u64>().ok())
            .map(Epoch)
            .unwrap_or(Epoch::ZERO)
    }

    fn write_epoch(&self, bot_id: &str, epoch: Epoch) -> Result<(), StoreError> {
        write_atomic(&self.epoch_path(bot_id), epoch.get().to_string().as_bytes())
    }

    fn read_all(&self) -> Result<Vec<StoredSnapshot>, StoreError> {
        let dir = self.snapshots_dir();
        let mut out = Vec::new();
        let Ok(entries) = std::fs::read_dir(&dir) else {
            return Ok(out);
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let Ok(bytes) = std::fs::read(&path) else {
                continue;
            };
            if let Ok(stored) = serde_json::from_slice::<StoredSnapshot>(&bytes) {
                out.push(stored);
            }
        }
        Ok(out)
    }

    fn write_stored(&self, stored: &StoredSnapshot) -> Result<(), StoreError> {
        let bytes =
            serde_json::to_vec_pretty(stored).map_err(|e| StoreError::Codec(e.to_string()))?;
        write_atomic(&self.snapshot_path(&stored.meta.id), &bytes)
    }

    /// Persist `snapshot` under a gateway-issued `id` (managed mode): the
    /// gateway's `SnapshotStore` is authoritative for ids, epochs and
    /// retention; the local file is a restore mirror so a successor pod —
    /// which shares this root — can `get` by the id `Activate` carried.
    /// No epoch check and no id allocation: the gateway already fenced.
    pub fn mirror(&self, id: &SnapshotId, snapshot: &BotSnapshot) -> Result<(), StoreError> {
        let size_bytes = snapshot.encode()?.len() as u64;
        let stored = StoredSnapshot {
            meta: SnapshotMeta {
                id: id.clone(),
                bot_id: snapshot.bot_id.clone(),
                epoch: snapshot.epoch,
                seq: snapshot.seq,
                quality: snapshot.quality,
                reason: snapshot.reason,
                taken_at_unix_ms: snapshot.taken_at_unix_ms,
                size_bytes,
                sha256: String::new(),
                deleted_at_unix_ms: None,
            },
            snapshot: snapshot.clone(),
        };
        self.write_stored(&stored)
    }
}

/// Bot/snapshot ids are file names — keep them filesystem-safe.
fn sanitize(id: &str) -> String {
    id.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// `write tmp + rename` so readers never see a partial file.
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), StoreError> {
    let parent = path
        .parent()
        .ok_or_else(|| StoreError::Backend(format!("no parent for {}", path.display())))?;
    std::fs::create_dir_all(parent)
        .map_err(|e| StoreError::Backend(format!("mkdir {}: {e}", parent.display())))?;
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, bytes)
        .map_err(|e| StoreError::Backend(format!("write {}: {e}", tmp.display())))?;
    std::fs::rename(&tmp, path)
        .map_err(|e| StoreError::Backend(format!("rename {}: {e}", path.display())))
}

#[async_trait]
impl SnapshotStore for LocalSnapshotStore {
    async fn record_epoch(&self, bot_id: &str, epoch: Epoch) -> Result<(), StoreError> {
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        if epoch > self.read_epoch(bot_id) {
            self.write_epoch(bot_id, epoch)?;
        }
        Ok(())
    }

    async fn current_epoch(&self, bot_id: &str) -> Result<Option<Epoch>, StoreError> {
        let epoch = self.read_epoch(bot_id);
        Ok((epoch > Epoch::ZERO).then_some(epoch))
    }

    async fn put(&self, snapshot: &BotSnapshot) -> Result<SnapshotId, StoreError> {
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        let current = self.read_epoch(&snapshot.bot_id);
        if snapshot.epoch != current {
            return Err(StoreError::Fenced(Fenced {
                expected: current,
                got: snapshot.epoch,
            }));
        }
        let payload = snapshot.encode()?;
        let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let id = SnapshotId::new(format!(
            "snap_{:013}_{}_{:04}",
            unix_ms().max(0) as u64,
            std::process::id(),
            seq
        ));
        let meta = SnapshotMeta {
            id: id.clone(),
            bot_id: snapshot.bot_id.clone(),
            epoch: snapshot.epoch,
            seq: snapshot.seq,
            quality: snapshot.quality,
            reason: snapshot.reason,
            taken_at_unix_ms: snapshot.taken_at_unix_ms,
            size_bytes: payload.len() as u64,
            sha256: String::new(),
            deleted_at_unix_ms: None,
        };
        let stored = StoredSnapshot {
            meta,
            snapshot: snapshot.clone(),
        };
        self.write_stored(&stored)?;
        Ok(id)
    }

    async fn get(&self, id: &SnapshotId) -> Result<StoredSnapshot, StoreError> {
        let bytes =
            std::fs::read(self.snapshot_path(id)).map_err(|_| StoreError::NotFound(id.clone()))?;
        serde_json::from_slice(&bytes).map_err(|e| StoreError::Codec(e.to_string()))
    }

    async fn latest(
        &self,
        bot_id: &str,
        quality: Option<SnapshotQuality>,
    ) -> Result<Option<StoredSnapshot>, StoreError> {
        Ok(self
            .read_all()?
            .into_iter()
            .filter(|s| {
                s.meta.bot_id == bot_id
                    && s.meta.deleted_at_unix_ms.is_none()
                    && quality.is_none_or(|q| s.meta.quality == q)
            })
            .max_by_key(|s| (s.meta.taken_at_unix_ms, s.meta.seq, s.meta.id.0.clone())))
    }

    async fn list(
        &self,
        bot_id: &str,
        include_deleted: bool,
    ) -> Result<Vec<SnapshotMeta>, StoreError> {
        let mut metas: Vec<SnapshotMeta> = self
            .read_all()?
            .into_iter()
            .filter(|s| {
                s.meta.bot_id == bot_id && (include_deleted || s.meta.deleted_at_unix_ms.is_none())
            })
            .map(|s| s.meta)
            .collect();
        metas.sort_by_key(|m| std::cmp::Reverse((m.taken_at_unix_ms, m.seq)));
        Ok(metas)
    }

    async fn soft_delete(&self, bot_id: &str, at_unix_ms: i64) -> Result<(), StoreError> {
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        for mut stored in self.read_all()? {
            if stored.meta.bot_id == bot_id && stored.meta.deleted_at_unix_ms.is_none() {
                stored.meta.deleted_at_unix_ms = Some(at_unix_ms);
                self.write_stored(&stored)?;
            }
        }
        Ok(())
    }

    async fn undelete(&self, bot_id: &str) -> Result<(), StoreError> {
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        for mut stored in self.read_all()? {
            if stored.meta.bot_id == bot_id && stored.meta.deleted_at_unix_ms.is_some() {
                stored.meta.deleted_at_unix_ms = None;
                self.write_stored(&stored)?;
            }
        }
        Ok(())
    }

    async fn purge(&self, before_unix_ms: i64) -> Result<u64, StoreError> {
        let mut removed = 0;
        for stored in self.read_all()? {
            if stored
                .meta
                .deleted_at_unix_ms
                .is_some_and(|at| at <= before_unix_ms)
            {
                let _ = std::fs::remove_file(self.snapshot_path(&stored.meta.id));
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Keep the latest `keep_clean` clean snapshots plus the newest
    /// checkpoint per bot (docs/04 §4.7 invariant).
    async fn gc(&self, bot_id: &str, keep_clean: usize) -> Result<u64, StoreError> {
        let mut stored: Vec<StoredSnapshot> = self
            .read_all()?
            .into_iter()
            .filter(|s| s.meta.bot_id == bot_id && s.meta.deleted_at_unix_ms.is_none())
            .collect();
        stored.sort_by_key(|s| (s.meta.taken_at_unix_ms, s.meta.seq));
        let mut keep: Vec<SnapshotId> = stored
            .iter()
            .filter(|s| s.meta.quality == SnapshotQuality::Clean)
            .rev()
            .take(keep_clean)
            .map(|s| s.meta.id.clone())
            .collect();
        if let Some(checkpoint) = stored
            .iter()
            .rev()
            .find(|s| s.meta.quality == SnapshotQuality::Checkpoint)
        {
            keep.push(checkpoint.meta.id.clone());
        }
        let mut removed = 0;
        for stored in stored {
            if !keep.contains(&stored.meta.id) {
                let _ = std::fs::remove_file(self.snapshot_path(&stored.meta.id));
                removed += 1;
            }
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    //! Golden-restore coverage: a `RuntimeCheckpoint` assembled into a
    //! `BotSnapshot` must restore bit-for-bit, and the file store must
    //! enforce the epoch-conditional fencing contract.
    use super::*;
    use xenomit_behavior_engine::ModuleState;
    use xenomit_bot_runtime::handoff::RestorePolicy;

    fn checkpoint() -> RuntimeCheckpoint {
        RuntimeCheckpoint {
            schema: RuntimeCheckpoint::SCHEMA,
            module_id: Some("kill_collect".into()),
            module: Some(ModuleState {
                schema: 1,
                settings_fingerprint: Some("fp:abc".into()),
                degraded: false,
                issues: vec![],
                data: serde_json::json!({
                    "run_key": "rk-1",
                    "current_step": "collect",
                    "attempt": 2,
                    "results": {"hunt": {"done": true}},
                    "entered_elapsed_ms": 4200
                }),
            }),
            rules: serde_json::json!({
                "attack-npc": { "kind": "builtin", "death_count": 7, "next_run_elapsed_ms": 900 }
            }),
            run_elapsed_ms: 61_000,
            game_hint: Some(serde_json::json!({"map_id": 16, "hero_x": 1200, "hero_y": 3400})),
            issues: vec!["deferred refresh".into()],
        }
    }

    fn epoch_store() -> (tempfile::TempDir, LocalSnapshotStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalSnapshotStore::new(dir.path().to_path_buf());
        (dir, store)
    }

    #[test]
    fn golden_restore_round_trips_runtime_checkpoint() {
        let (_dir, store) = epoch_store();
        block_on(store.record_epoch("bot_t", Epoch(1))).unwrap();
        let snapshot = assemble(
            "bot_t",
            Epoch(1),
            SnapshotReason::Upgrade,
            SnapshotQuality::Clean,
            Some(checkpoint()),
            None,
            0,
            "running",
            "kill_collect",
        );
        let id = block_on(store.put(&snapshot)).unwrap();
        let stored = block_on(store.get(&id)).unwrap();
        let plan = restore_plan(&stored, RestorePolicy::BestEffort).unwrap();
        let restored = plan.pending.checkpoint;
        let original = checkpoint();
        assert_eq!(restored.module_id, original.module_id);
        assert_eq!(
            restored.module.as_ref().unwrap().data["current_step"],
            "collect"
        );
        assert_eq!(restored.run_elapsed_ms, original.run_elapsed_ms);
        assert_eq!(restored.rules["attack-npc"]["death_count"], 7);
        assert_eq!(restored.game_hint, original.game_hint);
        assert_eq!(restored.issues, original.issues);
        assert_eq!(plan.pending.snapshot_id.as_deref(), Some(id.0.as_str()));
        assert_eq!(plan.pending.quality, CheckpointQuality::Clean);
        // Typed mirrors kept the doc shape.
        assert_eq!(stored.snapshot.module.as_ref().unwrap().id, "kill_collect");
        assert_eq!(
            stored
                .snapshot
                .module
                .as_ref()
                .unwrap()
                .sequence
                .as_ref()
                .unwrap()
                .step
                .as_deref(),
            Some("collect")
        );
        assert_eq!(
            stored.snapshot.behaviors["attack-npc"].state["death_count"],
            7
        );
        assert_eq!(stored.snapshot.game_hint.unwrap().map_id, Some(16));
        assert_eq!(stored.snapshot.stats.deaths, 7);
    }

    #[test]
    fn restore_rejects_newer_schema() {
        let mut snapshot = BotSnapshot::new(
            "bot_t",
            Epoch(1),
            SnapshotReason::Upgrade,
            SnapshotQuality::Clean,
            unix_ms(),
        );
        snapshot.schema = SNAPSHOT_SCHEMA + 1;
        let stored = StoredSnapshot {
            meta: SnapshotMeta {
                id: SnapshotId::new("snap_x"),
                bot_id: "bot_t".into(),
                epoch: Epoch(1),
                seq: 0,
                quality: SnapshotQuality::Clean,
                reason: SnapshotReason::Upgrade,
                taken_at_unix_ms: unix_ms(),
                size_bytes: 0,
                sha256: String::new(),
                deleted_at_unix_ms: None,
            },
            snapshot,
        };
        assert!(restore_plan(&stored, RestorePolicy::Strict).is_err());
    }

    #[test]
    fn restore_bare_module_state_uses_typed_sections() {
        // A writer that stored a bare ModuleState (not the envelope) still
        // restores: behaviors come from the typed section.
        let mut snapshot = BotSnapshot::new(
            "bot_t",
            Epoch(1),
            SnapshotReason::Upgrade,
            SnapshotQuality::Clean,
            unix_ms(),
        );
        snapshot.module = Some(ModuleSection {
            id: "kill_collect".into(),
            state_version: 1,
            state: serde_json::json!({
                "schema": 1,
                "settings_fingerprint": "fp:abc",
                "degraded": false,
                "issues": [],
                "data": {"deaths": 3}
            }),
            sequence: None,
        });
        snapshot.behaviors.insert(
            "collect".into(),
            BehaviorSection {
                state_version: 1,
                state: serde_json::json!({"collected": 12}),
            },
        );
        let stored = StoredSnapshot {
            meta: SnapshotMeta {
                id: SnapshotId::new("snap_bare"),
                bot_id: "bot_t".into(),
                epoch: Epoch(1),
                seq: 0,
                quality: SnapshotQuality::Clean,
                reason: SnapshotReason::Upgrade,
                taken_at_unix_ms: unix_ms(),
                size_bytes: 0,
                sha256: String::new(),
                deleted_at_unix_ms: None,
            },
            snapshot,
        };
        let plan = restore_plan(&stored, RestorePolicy::Strict).unwrap();
        assert_eq!(
            plan.pending.checkpoint.module_id.as_deref(),
            Some("kill_collect")
        );
        assert_eq!(plan.pending.checkpoint.rules["collect"]["collected"], 12);
    }

    #[test]
    fn epoch_fencing_rejects_stale_put_and_bump_fences_writer() {
        let (_dir, store) = epoch_store();
        let snapshot = BotSnapshot::new(
            "bot_t",
            Epoch(1),
            SnapshotReason::Upgrade,
            SnapshotQuality::Clean,
            unix_ms(),
        );
        // No epoch recorded yet: 0 is current, epoch 1 is stale.
        assert!(matches!(
            block_on(store.put(&snapshot)),
            Err(StoreError::Fenced(_))
        ));
        block_on(store.record_epoch("bot_t", Epoch(1))).unwrap();
        block_on(store.put(&snapshot)).unwrap();
        // A successor arms epoch 2 — every epoch-1 write now fences.
        block_on(store.record_epoch("bot_t", Epoch(2))).unwrap();
        assert!(matches!(
            block_on(store.put(&snapshot)),
            Err(StoreError::Fenced(Fenced {
                expected: Epoch(2),
                got: Epoch(1)
            }))
        ));
        assert_eq!(
            block_on(store.current_epoch("bot_t")).unwrap(),
            Some(Epoch(2))
        );
        // Epoch writes are monotonic — a stale record does not rewind.
        block_on(store.record_epoch("bot_t", Epoch(1))).unwrap();
        assert_eq!(
            block_on(store.current_epoch("bot_t")).unwrap(),
            Some(Epoch(2))
        );
    }

    #[test]
    fn latest_listing_gc_and_soft_delete() {
        let (_dir, store) = epoch_store();
        block_on(store.record_epoch("bot_t", Epoch(1))).unwrap();
        let snap =
            |reason, quality| BotSnapshot::new("bot_t", Epoch(1), reason, quality, unix_ms());
        let checkpoint = block_on(store.put(&snap(
            SnapshotReason::Checkpoint,
            SnapshotQuality::Checkpoint,
        )))
        .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let clean1 =
            block_on(store.put(&snap(SnapshotReason::Upgrade, SnapshotQuality::Clean))).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let clean2 =
            block_on(store.put(&snap(SnapshotReason::Upgrade, SnapshotQuality::Clean))).unwrap();
        let latest = block_on(store.latest("bot_t", None)).unwrap().unwrap();
        assert_eq!(latest.meta.id, clean2);
        assert_eq!(block_on(store.list("bot_t", false)).unwrap().len(), 3);

        // Soft delete hides everything from latest/list without deleting.
        block_on(store.soft_delete("bot_t", unix_ms())).unwrap();
        assert!(block_on(store.latest("bot_t", None)).unwrap().is_none());
        assert_eq!(block_on(store.list("bot_t", false)).unwrap().len(), 0);
        assert_eq!(block_on(store.list("bot_t", true)).unwrap().len(), 3);
        block_on(store.undelete("bot_t")).unwrap();
        assert!(block_on(store.latest("bot_t", None)).unwrap().is_some());

        // GC keeps keep_clean clean snapshots + the newest checkpoint.
        block_on(store.gc("bot_t", 1)).unwrap();
        let metas = block_on(store.list("bot_t", false)).unwrap();
        assert_eq!(metas.len(), 2);
        let ids: Vec<String> = metas.iter().map(|m| m.id.0.clone()).collect();
        assert!(ids.contains(&clean2.0));
        assert!(ids.contains(&checkpoint.0));
        assert!(!ids.contains(&clean1.0));
    }
}
