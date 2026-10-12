#!/usr/bin/env bash
# Materialize the multi-repo build workspace for the botd image.
#
# Run from the parent workspace directory (the Docker build context). Ensures:
#   ./xenomit-botd                 checked out at $GITHUB_SHA
#   ./xenomit-<dep>                checked out at the immutable SHA in
#                                  ci/source-pins.json (resolved relative to
#                                  this script, so it also works standalone)
#
# Sibling repositories are public and cloned unauthenticated from github.com.
set -euo pipefail

PINS_FILE="${PINS_FILE:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/source-pins.json}"
WORKSPACE="${1:-$(pwd)}"
GIT_BASE="${GIT_BASE:-https://github.com}"

if ! command -v jq >/dev/null 2>&1; then
  echo "error: jq is required to read $PINS_FILE" >&2
  exit 1
fi

checkout_at() {
  local dir="$1" repo="$2" sha="$3"
  if [ ! -d "$dir/.git" ]; then
    mkdir -p "$dir"
    git -C "$dir" init --quiet
    git -C "$dir" remote add origin "$GIT_BASE/$repo.git"
  fi
  # Fetch the exact pin only if the local clone does not already have it.
  git -C "$dir" cat-file -e "$sha^{commit}" 2>/dev/null \
    || git -C "$dir" fetch --quiet --depth 1 origin "$sha"
  git -C "$dir" checkout --quiet --detach "$sha"
  local actual
  actual=$(git -C "$dir" rev-parse HEAD)
  if [ "$actual" != "$sha" ]; then
    echo "error: $dir pinned to $sha but HEAD is $actual" >&2
    exit 1
  fi
  echo "pinned $dir -> ${sha:0:12}"
}

# botd itself tracks the triggering commit, not a pin.
: "${GITHUB_SHA:?GITHUB_SHA is required}"
checkout_at "$WORKSPACE/xenomit-botd" "xenomit-do/xenomit-botd" "$GITHUB_SHA"

while IFS=$'\t' read -r dir repo sha; do
  if [[ ! "$dir" =~ ^xenomit-[a-z0-9-]+$ || "$repo" != "xenomit-do/$dir" || ! "$sha" =~ ^[0-9a-f]{40}$ ]]; then
    echo "error: invalid source pin: $dir $repo $sha" >&2
    exit 1
  fi
  checkout_at "$WORKSPACE/$dir" "$repo" "$sha"
done < <(jq -r '.pins | to_entries[] | [.key, .value.repository, .value.sha] | @tsv' "$PINS_FILE")

echo "workspace ready at $WORKSPACE"
