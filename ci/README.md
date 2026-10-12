# Image build sources

`.github/workflows/image.yml` builds, signs, and publishes the `botd` image.
The Docker build context is the parent workspace: `Dockerfile` reads sibling
checkouts (`xenomit-orchestrator/fleet-types`, the eight Rust path
dependencies, `xenomit-flash-client*`), not just this repo.

- `source-pins.json` — immutable commit pins for every sibling dependency.
- `checkout-sources.sh` — materializes the workspace: `xenomit-botd` at
  `GITHUB_SHA`, each public `xenomit-do/xenomit-*` dependency at its pinned SHA
  (repository and SHA are validated, then verified with `git rev-parse`).

## Updating pins

When a sibling repo's manifest changes (any `Cargo.toml` in the dependency
graph, or flash-client sources the Dockerfile copies):

1. Bump the corresponding `sha` in `source-pins.json` to a full commit SHA.
   Never pin branches or tags.
2. Materialize the pinned workspace locally:
   `GITHUB_SHA=$(git rev-parse HEAD) ./ci/checkout-sources.sh /tmp/botd-ws`
   (or run it inside a scratch dir with this repo cloned as `xenomit-botd`).
3. In that workspace, apply the Dockerfile's workspace trimming
   (`sed -i '/^members = \[/,/^\]/c\members = ["fleet-types"]'
   xenomit-orchestrator/Cargo.toml`) and regenerate/validate the lockfile:
   `cargo metadata --locked` — if it reports the lockfile is stale, run
   `cargo update --workspace` inside the pinned workspace and commit the
   resulting `Cargo.lock` together with the pin bump.
4. Keep `Cargo.lock` and `source-pins.json` consistent: `cargo build --locked`
   inside the image must resolve the same pinned set.
