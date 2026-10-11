#!/usr/bin/env python3
"""Credential-free image checks: python container/smoke.py [image]."""
import pathlib
import subprocess
import sys
import tempfile
import time
import uuid

image = sys.argv[1] if len(sys.argv) > 1 else "xenomit-botd:flash"

def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()

assert "USAGE:" in docker("run", "--rm", image, "--help")
checks = r'''
set -eu
test "$(id -u)" = 65534
for binary in /usr/local/bin/xenomit-botd /opt/xenomit-flash-client/build/libxenomit_flash_hook.so /opt/xenomit-flash-client/build/xenomit_flash_inject /opt/xenomit-flash-client/browser/node_modules/electron/dist/electron; do
  output=$(ldd "$binary")
  if printf '%s\n' "$output" | grep -q 'not found'; then printf '%s\n' "$output"; exit 1; fi
done
test "$(/opt/xenomit-flash-client/browser/node_modules/electron/dist/electron --no-sandbox --version)" = v11.5.0
! command -v cargo
! command -v gcc
'''
docker("run", "--rm", "--entrypoint", "/bin/bash", image, "-c", checks)

# Replace only botd to verify the real tini/Xvfb entrypoint's shutdown contract.
with tempfile.TemporaryDirectory(prefix="botd-container-smoke-") as directory:
    stub = pathlib.Path(directory) / "botd"
    stub.write_text('''#!/bin/bash
set -eu
test -S "/tmp/.X11-unix/X${DISPLAY#:}"
trap 'test -S "/tmp/.X11-unix/X${DISPLAY#:}"; echo drained-with-display; exit 0' TERM
echo display-ready
while true; do sleep 0.1; done
''', newline="\n")
    stub.chmod(0o755)
    name = "botd-smoke-" + uuid.uuid4().hex[:12]
    docker("run", "-d", "--name", name, "--mount",
           f"type=bind,src={stub},dst=/usr/local/bin/xenomit-botd,readonly", image)
    try:
        for _ in range(100):
            if "display-ready" in docker("logs", name):
                break
            time.sleep(0.1)
        else:
            raise AssertionError("virtual display never became ready")
        docker("stop", "--time", "10", name)
        assert docker("inspect", "--format", "{{.State.ExitCode}}", name) == "0"
        assert "drained-with-display" in docker("logs", name)
    finally:
        docker("rm", "-f", name)
print("PASS: non-root runtime, libraries, Electron, Xvfb startup and SIGTERM drain")
