# syntax=docker/dockerfile:1
# Build context: sibling repositories' parent directory. Linux amd64 only.
FROM node:22-bookworm-slim AS flash-build
RUN apt-get update && apt-get install -y --no-install-recommends build-essential cmake ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /work
COPY xenomit-flash-client-linux/hook xenomit-flash-client-linux/hook
COPY xenomit-flash-client-linux/third_party xenomit-flash-client-linux/third_party
COPY xenomit-flash-client-linux/scripts xenomit-flash-client-linux/scripts
COPY xenomit-flash-client/src xenomit-flash-client/src
COPY xenomit-flash-client/generated xenomit-flash-client/generated
COPY xenomit-flash-client/third_party xenomit-flash-client/third_party
RUN bash xenomit-flash-client-linux/scripts/build.sh && test -x xenomit-flash-client-linux/build/xenomit_flash_inject
COPY xenomit-flash-client-linux/browser/package.json xenomit-flash-client-linux/browser/package.json
RUN cd xenomit-flash-client-linux/browser && npm install --omit=dev && rm -rf /root/.npm
COPY xenomit-flash-client-linux/browser/src xenomit-flash-client-linux/browser/src
COPY xenomit-flash-client-linux/xenomit_flash_client xenomit-flash-client-linux/xenomit_flash_client
COPY xenomit-flash-client-linux/runtime/pepper.sha256 xenomit-flash-client-linux/runtime/pepper.sha256
RUN chmod +x xenomit-flash-client-linux/xenomit_flash_client

FROM rust:1.98-bookworm AS build
WORKDIR /work
COPY xenomit-game-api/Cargo.toml xenomit-game-api/Cargo.toml
COPY xenomit-game-api/src xenomit-game-api/src
COPY xenomit-backpage/Cargo.toml xenomit-backpage/Cargo.toml
COPY xenomit-backpage/src xenomit-backpage/src
COPY xenomit-behavior-engine/Cargo.toml xenomit-behavior-engine/Cargo.toml
COPY xenomit-behavior-engine/src xenomit-behavior-engine/src
COPY xenomit-packet-client/Cargo.toml xenomit-packet-client/Cargo.toml
COPY xenomit-packet-client/src xenomit-packet-client/src
COPY xenomit-core/Cargo.toml xenomit-core/Cargo.toml
COPY xenomit-core/src xenomit-core/src
COPY xenomit-bot-runtime/Cargo.toml xenomit-bot-runtime/Cargo.toml
COPY xenomit-bot-runtime/src xenomit-bot-runtime/src
COPY xenomit-mock-game/Cargo.toml xenomit-mock-game/Cargo.toml
COPY xenomit-mock-game/src xenomit-mock-game/src
COPY xenomit-orchestrator/Cargo.toml xenomit-orchestrator/Cargo.toml
COPY xenomit-orchestrator/fleet-types xenomit-orchestrator/fleet-types
COPY xenomit-botd/Cargo.toml xenomit-botd/Cargo.toml
COPY xenomit-botd/Cargo.lock xenomit-botd/Cargo.lock
COPY xenomit-botd/src xenomit-botd/src
COPY xenomit-botd/benches xenomit-botd/benches
RUN sed -i '/^members = \[/,/^\]/c\members = ["fleet-types"]' xenomit-orchestrator/Cargo.toml
WORKDIR /work/xenomit-botd
RUN cargo build --release --locked

FROM debian:bookworm-slim AS flash-runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash ca-certificates tini xvfb fonts-liberation \
    libgtk-3-0 libnss3 libasound2 libgbm1 libxss1 libxtst6 libdrm2 libgl1 \
    libx11-xcb1 libxcb-dri3-0 libatk-bridge2.0-0 \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /botd-state /home/botd /etc/xenomit \
    && chown -R 65534:65534 /botd-state /home/botd
COPY --from=flash-build /work/xenomit-flash-client-linux/build/libxenomit_flash_hook.so /opt/xenomit-flash-client/build/libxenomit_flash_hook.so
COPY --from=flash-build /work/xenomit-flash-client-linux/build/xenomit_flash_inject /opt/xenomit-flash-client/build/xenomit_flash_inject
COPY --from=flash-build /work/xenomit-flash-client-linux/browser /opt/xenomit-flash-client/browser
COPY --from=flash-build /work/xenomit-flash-client-linux/xenomit_flash_client /opt/xenomit-flash-client/xenomit_flash_client
COPY --from=flash-build /work/xenomit-flash-client-linux/runtime/pepper.sha256 /opt/xenomit-flash-client/runtime/pepper.sha256
COPY --chmod=755 xenomit-botd/container/entrypoint.sh /usr/local/bin/container-entrypoint
ENV HOME=/home/botd XENOMIT_FLASH_CLIENT_EXE=/opt/xenomit-flash-client/xenomit_flash_client \
    XENOMIT_FLASH_PLUGIN_PATH=/run/flash/libpepflashplayer.so \
    XENOMIT_BOTD_CONFIG=/etc/xenomit/botd.json XENOMIT_HEALTH_BIND=0.0.0.0:8080
RUN mkdir -p /tmp/.X11-unix && chmod 1777 /tmp/.X11-unix
WORKDIR /home/botd
USER 65534:65534
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/container-entrypoint"]

FROM build AS stripped
RUN strip /work/xenomit-botd/target/release/xenomit-botd

FROM build AS test
RUN cargo test --release --lib --locked

FROM flash-runtime AS runtime
COPY --from=stripped /work/xenomit-botd/target/release/xenomit-botd /usr/local/bin/xenomit-botd
EXPOSE 8080
