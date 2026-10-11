# xenomit-botd

Headless Xenomit bot daemon — orchestrator phase 1a
(`xenomit-orchestrator/docs/02-headless-bot.md`). It embeds
`xenomit-bot-runtime` (the same planner the GPUI app drives) and runs a bot
as a Linux/container-friendly process: **packet or Linux Flash client, no GPUI**.

```text
Boot → LoadConfig → Arm → Activate → GetSnapshot → Login → Attach →
       Restore → Run → (Drain → Snapshot → Logout) → exit
```

Phase 3 adds the graceful-handoff contract from
`xenomit-orchestrator/docs/03-graceful-handoff.md`: a predecessor pod drains
cooperatively, writes an epoch-conditional snapshot, logs out *before* any
successor may log in, reports `Drained`, and exits 0. A successor fetches
its snapshot before touching the game, restores module state plus the
unacked statistics tail, and continues with no duplicate event sequences.

## Usage

```sh
xenomit-botd                       # uses $XENOMIT_BOTD_CONFIG or /etc/xenomit/botd.json
xenomit-botd --config botd.json
xenomit-botd --print-schema        # emits the JSON Schema (schema/botd.schema.json)
xenomit-botd --help | --version
```

Config precedence: `--config` → `$XENOMIT_BOTD_CONFIG` →
`/etc/xenomit/botd.json`.

## Configuration

`botd.json` is schema `1`; the committed JSON Schema is
[`schema/botd.schema.json`](schema/botd.schema.json). Minimal standalone
config:

```json
{
  "schema": 1,
  "bot":    { "id": "bot_01h…", "tenant": "me" },
  "account": {
    "server": "int1.darkorbit.com",
    "auth": "session",
    "credentials": { "file": "/run/secrets/account.json" }
  },
  "client": { "kind": "packet" },
  "settings": { "active_module": "kill_collect" },
  "autostart": true
}
```

- `account.auth`: `"password"` (credentials file holds
  `{"username","password"}` or `user:pass`) or `"session"`
  (`{"dosid":"…"}` / raw dosid).
- `client.kind`: `"packet"` or `"flash"`. Flash requires Linux amd64 and a virtual X display.
- `settings` is the exact `BotSettings` (`modules.json`) shape the GUI
  persists; `{}` means defaults.
- Optional blocks: `network` (`proxy` secret ref, `require_proxy`),
  `control` (orchestrator gateway — **stubbed**, fails fast at Arm until the
  control stream lands), `panel` (`base_url` + `instance_token` secret ref,
  Agent Protocol v1), `vault` (declared, resolver returns *not
  implemented*), `telemetry.otlp_endpoint`, `health`,
  `testing` (mock-server endpoint overrides — never in production).
- Optional live status streaming is configured with environment variables:
  `XENOMIT_BOTD_STREAM_ADDR` (`host:port` for `xenomit-bot-stream` ingest)
  and `XENOMIT_BOTD_STREAM_TOKEN` (same token configured on the stream
  service). When unset, streaming is disabled. The publisher reconnects in
  the background and coalesces to the latest status frame; it sends bot id,
  tenant, epoch, per-process sequence, phase, active module, activity,
  balances, rates, uptime, and needs-human status. It never sends credentials
  or settings.
- `handoff`: `drain_deadline_s` (600) cooperative drain budget —
  paused while the module is in a `non_interruptible` section;
  `hard_deadline_s` (1800) wall-clock ceiling that force-stops and still
  writes a degraded snapshot; `checkpoint_interval_s` (15) periodic
  checkpoint cadence; `restore_policy` (`best_effort` default — corrupt
  snapshot logs a warning and starts fresh, or `strict` — unreadable
  snapshot exits 70 without logging in; a *missing* snapshot is always a
  fresh start).

`--print-schema` output and `schema/botd.schema.json` are regenerated via
`cargo run -- --print-schema > schema/botd.schema.json`.

## Secrets

Secrets are never inline. `SecretRef` is `{"file": "/path"}` (implemented)
or `{"vault": {"path": "kv/…"}}` (stub — clear *not implemented* error).
File refs are re-read per authentication attempt so rotated credentials
apply without restart; a transiently missing file falls back to the last
armed credentials. Secret material is `zeroize`d on drop and its `Debug`
output is redacted — credentials, dosid and tokens never reach logs or
panel payloads.

## Exit codes

| Code | Meaning |
|-----:|---------|
| 0    | Clean drain (SIGTERM / panel shutdown / host `Close`) |
| 1    | Crash or non-specific failure — restartable |
| 64   | Invalid config, unreadable secrets, unimplemented blocks |
| 65   | Credentials/session rejected — human re-provision |
| 66   | CAPTCHA required — human or captcha service |
| 67   | Session fenced by another login |
| 68   | Game version hash rejected — update required |
| 69   | Proxy required but unreachable |
| 70   | Strict-mode handoff snapshot unreadable |

## Health endpoints

Localhost-only HTTP (default `127.0.0.1:8080`, `health.bind`):

| Path        | 200 when | 503 when |
|-------------|----------|----------|
| `/startupz` | armed (secrets resolved) | still booting |
| `/readyz`   | session attached + data plane live | login/attach/drain or `needs_human` |
| `/livez`    | supervisor + expected worker heartbeats fresh | stalled > 60 s |

`/readyz` 503s carry `needs_human` (`credentials`/`captcha`/`fenced`/
`version`) and `error` detail.

## Supervisor behavior

- Per-tick: heartbeat, host-action drain, panel-command dispatch,
  client-liveness check, hangar-balance reconcile (60 s), stopped-state
  rules tick, panel state push.
- Dropped packet session → detach → re-login + re-attach after
  `reconnect_delay`, unless a `Disconnect` host action or a `needs_human`
  failure set the hold (then the daemon parks and reports instead of
  hammering login).
- `HostAction::Refresh`/`RefreshIdle` → in-process re-login (stop → login
  → attach → resume), `Close` → graceful drain, `Disconnect` → detach +
  hold.
- SIGTERM/SIGINT, a control-plane `Drain` order, or `Close` → graceful
  drain: stop module at a quiescent point → write snapshot → flush stats →
  log out → `Drained` report → exit 0; a second signal or SIGQUIT → abort.
- Every `handoff.checkpoint_interval_s` the worker emits a
  `quality=checkpoint` snapshot, persisted epoch-conditionally to the
  snapshot store (`snapshots/` under the state dir in the local build).
- Fencing: the control epoch is recorded at Activate; every snapshot write
  and the periodic fence probe compare against `current_epoch`. A stale pod
  exits 67 and writes nothing.
- Statistics: the runtime journal (`statistics/*.jsonl` + cursor sidecar)
  is the durable outbox — events are sequenced, replayed (`seq > cursor`)
  on restart, and acknowledged via panel `stat_ack`. Handoff checkpoints
  carry the unacked tail (≤ 1 000 events) so successors replay without
  loss or duplicate seqs.
- Panel commands: `start`, `stop`, `set_module`, `update_config`,
  `game_command`, `refresh_stats`, `reset_stats`, `shutdown` — all answered
  with `command_result`.

## Panel link (Agent Protocol v1)

With a `panel` block the daemon resolves `instance_token`, configures
`PanelLink` and spawns the runtime's agent task: it publishes `hello`
(instance identity + module catalog), `status`, `config` and forwards
statistics/log events. Payloads never carry session material.

## Testing overrides

`testing.backpage_base_url` redirects every Backpage call,
`testing.front_base_url` only the password-login leg, and
`testing.game_endpoint` (`host:port`) skips `maps.php` map discovery.
Together they point the daemon at `xenomit-mock-game`; see
[`tests/mock_game.rs`](tests/mock_game.rs).

## Docker

Multi-stage, distroless non-root. The daemon path-depends on sibling
crates, so build **from the workspace root**:

```sh
docker build -f xenomit-botd/Dockerfile -t xenomit-botd .
docker run --rm \
  -v /etc/xenomit:/etc/xenomit:ro \
  -v botd-state:/botd-state \
  xenomit-botd
```

## Development

```sh
cargo build
cargo test
cargo clippy --all-targets
cargo run -- --print-schema > schema/botd.schema.json   # regenerate schema

# end-to-end against an embedded mock game server (no external services):
cargo test --test mock_game

# 50-bot fleet benchmark (spawns real botd processes vs one mock):
cargo bench --bench fleet -- --bots 50 --duration-secs 300
# writes BENCHMARK.md; see that file for the latest measured numbers
```

## Layout

| Module | Role |
|--------|------|
| `cli`    | arg parsing → `Daemon::production` |
| `config` | `botd.json` types, validation, JSON Schema, path precedence |
| `secrets` | `SecretRef`, `SecretResolver` (file + vault stub), `SecretString` (zeroize + redacted Debug), credential/token parsers |
| `error`  | `BotdError` taxonomy → `ExitCode` |
| `state`  | `Phase` machine + panel vocabulary |
| `daemon` | startup state machine, supervisor loop, host-action handling, re-attach, drain, signals, epoch fencing, snapshot lifecycle |
| `session`| `HeadlessSessionProvider` (password / dosid, mock-aware) |
| `host`   | `HeadlessHostActions` queue |
| `settings` | read-only `ConfigSettingsStore` |
| `stats`  | `MetricsSink` — telemetry counted per event; the runtime journal owns the durable outbox |
| `snapshot` | `LocalSnapshotStore` (filesystem), `BotSnapshot` assembly from runtime checkpoints + stats tail, restore planning, epoch-conditional writes |
| `health` | `/startupz` `/readyz` `/livez` HTTP on a small tokio runtime |
| `heartbeat` | supervisor/worker beats + `HeartbeatClient` decorator |
| `control`| `ControlPlane` trait: `StandaloneControl`, `LocalControl` (scripted test fake), fail-fast `GatewayControl` stub |
| `proxy`  | proxy spec parsing + egress reachability check |
| `panel`  | hello/config/status payload builders (Agent Protocol v1) |

## Headless Linux Flash container

Build from the sibling repositories' parent directory:

```sh
docker build --platform linux/amd64 -f xenomit-botd/Dockerfile -t xenomit-botd:flash .
```

The multi-stage image builds botd, the native capture hook, the SWF bridge
injector, and the pinned Electron 11.5.0 host. Debian slim supplies only the
player's runtime libraries, fonts, Xvfb, and tini; no desktop environment,
Node toolchain, compiler, or GPUI is installed in the final image.

Set `client.kind` to `"flash"` in botd.json and `state_dir` to `/botd-state`.
Mount your separately supplied PPAPI Flash **32.0.0.371** plugin read-only;
its hash must match xenomit-flash-client-linux/runtime/pepper.sha256. The
plugin and account credentials are never baked into the image.

```sh
docker run --rm --name xenomit-botd --platform linux/amd64 \
  --shm-size=256m --stop-timeout=1800 \
  -p 127.0.0.1:8080:8080 \
  --mount type=bind,src="$PWD/botd.json",dst=/etc/xenomit/botd.json,readonly \
  --mount type=bind,src="$PWD/account.json",dst=/run/secrets/account.json,readonly \
  --mount type=bind,src="$PWD/libpepflashplayer.so",dst=/run/flash/libpepflashplayer.so,readonly \
  --mount type=volume,src=xenomit-botd-state,dst=/botd-state \
  xenomit-botd:flash
```

The process runs as uid/gid 65534. Bind mounts must be readable by that uid;
bind-mounted state must be writable. HOME and player caches live under
/home/botd. Xvfb needs writable /tmp and Flash uses /dev/shm. No host display,
GPU device, privileged mode, or host IPC is required. The player stays mapped
on the virtual display so Flash/Flex can render and publish live frames.
The entrypoint keeps Xvfb alive during SIGTERM drain; the last game-client
reference kills/reaps the player's process group and removes bridge files.

Flash attach waits up to 120 seconds for fresh native frames and a successful
command-bridge ping. The host HTML is fulfilled locally under the authenticated
HTTP(S) page origin, which Pepper requires for ExternalInterface callbacks;
bootstrap HTML is not uploaded to the game server. Allow authentication
plus this interval in startup probes using `/readyz`; `/startupz` only
confirms Arm completed. Delay liveness checks until startup succeeds. `/startupz`, `/readyz`, `/livez` are
available on port 8080; readiness means the game bridge attached.
Flash rejects botd `network.proxy`/`require_proxy` and mock game endpoint
overrides; use packet mode for those features, or enforce egress outside
the container. The legacy Electron host disables its Chromium sandbox,
so retain the container's default seccomp/capability isolation and non-root uid.

Smoke check without account credentials or the proprietary plugin:

```sh
docker run --rm xenomit-botd:flash --help
docker run --rm --entrypoint /opt/xenomit-flash-client/browser/node_modules/electron/dist/electron xenomit-botd:flash --no-sandbox --version
```

A live account and the pinned plugin are required
to verify actual movie playback, capture, and game operations.

Run the repeatable image checks with python container/smoke.py xenomit-botd:flash from this repository. They also verify SIGTERM keeps the virtual display alive until drain finishes. Linux unit tests can run with docker build --platform linux/amd64 --target test --output type=cacheonly -f xenomit-botd/Dockerfile . from the sibling parent directory.

Live validation (2026-10-11): the image authenticated a session on `tr3.darkorbit.com`
with the pinned Pepper plugin, reached sustained `/readyz=ready` and `/livez=alive`,
published advancing v2 native frames on map 2-2, and completed a read-only
`FINVOKE xenomit clientFeatures []` on the game frame. Automation was disabled.
Linux native frames now carry validated fresh obstacle geometry, including
circles, rectangles, polygons and inverted zones. Geometry is checked against
the current map and capture timestamp; movement remains gated if it is stale
or unavailable. This validates hosting/capture/command plumbing and geometry
delivery, not a complete autonomous gameplay run.

Performance checks use `container/perf-probe.js` inside the image and
`examples/flash_benchmark.rs` against the player's PID. The former reports
capture microseconds and actual Flash FPS; the latter measures the real
`GameClient::status()` path and verifies typed geometry delivery. These are
separate measurements. See the Linux host's PERFORMANCE.md for live results.
