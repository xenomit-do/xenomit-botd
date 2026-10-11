# xenomit-botd fleet benchmark

Measured 2026-10-04 on the local machine against **xenomit-mock-game** (embedded mock Backpage + game server) — not the real DarkOrbit services.

| Setting | Value |
|---|---|
| bots | 50 |
| duration | 300s |
| stagger | 40ms + jitter |
| sample interval | 2s |
| module | kill_collect |
| botd binary | `C:\Users\Yagiz\Desktop\repos\xenomit-botd\target\release\xenomit-botd.exe` |
| build profile | release |
| spawn window | 7.1734197s |

## Per-bot resources (sysinfo)

| Metric | p50 | p95 | max |
|---|---:|---:|---:|
| peak RSS (MiB) | 20.7 | 21.3 | 21.3 |
| mean RSS (MiB) | 20.5 | 20.8 | 21.0 |
| mean CPU (% of one core) | 0.54 | 0.67 | 0.68 |
| login time spawn→ready (ms) | 4209 | 7047 | 7236 |

Fleet mean CPU total: **27.0%** of one core = **1.7%** of machine capacity (16 logical cores, sysinfo `cpu_usage` scale).

## Mock-side activity

| Counter | Value |
|---|---:|
| backpage HTTP requests | 1131 |
| game logins accepted | 71 |
| game logins rejected | 0 |
| live sessions at stop | 39 |
| packets received (live sessions) | 34825 |
| packets sent (live sessions) | 140774 |

## Outcome

| Result | Count |
|---|---:|
| reached ready (/readyz 200) | 50 / 50 |
| clean exit (code 0) | 50 / 50 |
| failed | 0 |

Notes: per-tick durations are not yet exposed by botd (no module timing metric) — activity is evidenced by the packet counters above. The mock adds no network latency and runs on localhost, so real-ISP numbers will be higher; treat this as a lower bound for sizing.
