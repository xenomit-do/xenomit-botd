# Soak results

docs/03 §3.10 requires a soak comparing an interrupted run (drain → successor
restores and continues) against an uninterrupted control; the event stream
and stat totals must agree.

## `soak_interrupted_run_keeps_event_stream_continuous`

`tests/handoff.rs`. Pod A runs `kill_collect` against `xenomit-mock-game`
for half the window, receives a `Drain` order (`deadline 10 s`, hard 30 s),
writes a snapshot, logs out, reports `Drained`, exits 0. Pod B activates
epoch 2 on the same snapshot store with `snapshot_id` from the `Drained`
report, restores, runs the remaining half, then SIGTERM-drains.

Asserted:

- predecessor journal non-empty, successor journal non-empty
- every successor event has `seq > 0`
- the combined seq stream (A journal + B journal, which includes the
  replayed unacked tail) contains **no duplicates**
- B's max seq ≥ A's max seq (sequence never regresses)
- exactly 2 game logins — predecessor logged out before successor logged
  in; fencing prevented any overlap

Window length is parameterised: default 8 s, override with
`BOTD_SOAK_SECS=<n>` for a longer soak:

```sh
BOTD_SOAK_SECS=600 cargo test --test handoff soak_ -- --nocapture
```

## Latest run

2026-07-03 · `BOTD_SOAK_SECS` unset (8 s window) ·
`cargo test --test handoff` — **10/10 pass**, soak included. The same suite
covers the rest of the kill matrix: abort mid-run keeps the last
checkpoint, SIGTERM drains clean, kill before Activate never logs in, stale
epoch and fenced-during-drain both exit 67 without a snapshot write, strict
corrupt snapshot exits 70, best-effort corrupt and missing snapshots start
fresh.
