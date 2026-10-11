//! Best-effort publisher for the independent `xenomit-bot-stream` service.
//!
//! The supervisor only replaces a Tokio watch value. A background task owns
//! all socket I/O and keeps only the newest update while the service is
//! disconnected or slow, so streaming cannot backpressure bot work.

use std::net::SocketAddr;
use std::time::Duration;

use serde_json::Value;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::watch;
use xenomit_core::logging::tracing::{info, warn};
use zeroize::Zeroizing;

const RECONNECT_DELAY: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct StreamPublisher {
    latest: watch::Sender<Value>,
}

impl StreamPublisher {
    /// Spawn a reconnecting publisher when `XENOMIT_BOTD_STREAM_ADDR` is set.
    /// `XENOMIT_BOTD_STREAM_TOKEN` is required whenever the endpoint is set.
    pub fn spawn(runtime: &tokio::runtime::Handle) -> Option<Self> {
        let address = std::env::var("XENOMIT_BOTD_STREAM_ADDR").ok()?;
        let token = match std::env::var("XENOMIT_BOTD_STREAM_TOKEN") {
            Ok(token) if !token.is_empty() && !token.contains(['\r', '\n']) => {
                Zeroizing::new(token)
            }
            _ => {
                warn!(
                    "stream address configured without XENOMIT_BOTD_STREAM_TOKEN; publisher disabled"
                );
                return None;
            }
        };
        let address = match address.parse::<SocketAddr>() {
            Ok(address) => address,
            Err(error) => {
                warn!(%error, "invalid XENOMIT_BOTD_STREAM_ADDR; publisher disabled");
                return None;
            }
        };
        let (latest, receiver) = watch::channel(Value::Null);
        runtime.spawn(publish_loop(address, token, receiver));
        info!(%address, "stream publisher started");
        Some(Self { latest })
    }

    /// Replace the pending payload without waiting for the network task.
    pub fn publish(&self, value: Value) {
        self.latest.send_replace(value);
    }
}

async fn publish_loop(
    address: SocketAddr,
    token: Zeroizing<String>,
    mut latest: watch::Receiver<Value>,
) {
    loop {
        match TcpStream::connect(address).await {
            Ok(mut socket) => {
                if socket.write_all(b"AUTH ").await.is_err()
                    || socket.write_all(token.as_bytes()).await.is_err()
                    || socket.write_all(b"\n").await.is_err()
                {
                    tokio::time::sleep(RECONNECT_DELAY).await;
                    continue;
                }
                info!(%address, "connected to stream service");
                loop {
                    if latest.changed().await.is_err() {
                        return;
                    }
                    let payload = latest.borrow_and_update().clone();
                    if payload.is_null() {
                        continue;
                    }
                    let mut line = match serde_json::to_vec(&payload) {
                        Ok(line) => line,
                        Err(error) => {
                            warn!(%error, "could not serialize stream update");
                            continue;
                        }
                    };
                    line.push(b'\n');
                    if socket.write_all(&line).await.is_err() {
                        warn!(%address, "stream service disconnected");
                        break;
                    }
                }
            }
            Err(error) => warn!(%address, %error, "stream service unavailable"),
        }
        tokio::time::sleep(RECONNECT_DELAY).await;
    }
}
