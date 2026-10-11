//! Secret references and resolvers (`docs/09-security.md` §9.2).
//!
//! Config carries *references* (`{"file": "/run/secrets/x"}` or
//! `{"vault": {"path": "kv/bots/x"}}`), never secret material. Resolution
//! happens at `Arm` through [`SecretResolver`]: files read from disk,
//! `vault` refs go through [`VaultSecretResolver`]'s KV v2 HTTP reads.
//! Every resolved value lives in [`SecretString`] — zeroized on drop and
//! redacted in `Debug`/`Display`.
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use schemars::JsonSchema;
use serde::Deserialize;
use zeroize::Zeroizing;

use crate::config::AuthMode;

/// Reference to secret material outside the config.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SecretRef {
    /// Vault KV path — resolved through the configured `vault` block.
    Vault {
        /// KV v2 data path, e.g. `secret/data/bots/<bot-id>/account`.
        path: String,
    },
    /// Local file holding the secret (dev, tests, projected volumes).
    File(PathBuf),
}

/// A resolved secret: zeroized on drop, never printable.
#[derive(Clone)]
pub struct SecretString(Zeroizing<String>);

impl SecretString {
    pub fn new(value: impl Into<String>) -> Self {
        Self(Zeroizing::new(value.into()))
    }
    /// Raw value — only at the authentication/pairing boundary.
    pub fn reveal(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretString(<redacted>)")
    }
}

impl fmt::Display for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// Why a [`SecretRef`] could not be resolved.
#[derive(Debug)]
pub enum SecretError {
    /// Transient — the projected file or Vault may appear shortly; Arm retries.
    Unavailable(String),
    /// Permanent — fix the reference or the file's contents.
    Invalid(String),
}

impl SecretError {
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Unavailable(_))
    }
}

impl fmt::Display for SecretError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable(msg) | Self::Invalid(msg) => f.write_str(msg),
        }
    }
}

impl std::error::Error for SecretError {}

/// Resolves [`SecretRef`]s to [`SecretString`]s. Called at `Arm` and again
/// when a consumer re-authenticates (a Vault-backed resolver may rotate).
pub trait SecretResolver: Send + Sync {
    fn resolve(&self, reference: &SecretRef) -> Result<SecretString, SecretError>;
}

const MAX_SECRET_BYTES: u64 = 64 * 1024;

/// Dev/test resolver: `file` refs read from disk, `vault` refs rejected.
pub struct FileSecretResolver;

impl FileSecretResolver {
    fn read(path: &Path) -> Result<SecretString, SecretError> {
        let metadata = std::fs::metadata(path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                SecretError::Unavailable(format!("secret file {} is missing", path.display()))
            } else {
                SecretError::Unavailable(format!(
                    "secret file {} is unreadable: {error}",
                    path.display()
                ))
            }
        })?;
        if metadata.len() > MAX_SECRET_BYTES {
            return Err(SecretError::Invalid(format!(
                "secret file {} exceeds {} bytes",
                path.display(),
                MAX_SECRET_BYTES
            )));
        }
        let text = std::fs::read_to_string(path).map_err(|error| {
            SecretError::Unavailable(format!(
                "secret file {} is unreadable: {error}",
                path.display()
            ))
        })?;
        Ok(SecretString::new(text))
    }
}

impl SecretResolver for FileSecretResolver {
    fn resolve(&self, reference: &SecretRef) -> Result<SecretString, SecretError> {
        match reference {
            SecretRef::File(path) => Self::read(path),
            SecretRef::Vault { path } => Err(SecretError::Invalid(format!(
                "vault secret {path} requires a configured vault block"
            ))),
        }
    }
}

/// Vault backend (docs/09 §9.2): KV v2 reads over the HTTP API,
/// authenticating with the projected service-account token through the
/// JWT/k8s auth mount. The client token is cached and renewed until
/// Vault stops granting renewal, then re-issued by a fresh login.
///
/// `resolve` is synchronous at the trait boundary (callers hold no
/// runtime) — each call runs its I/O on a short-lived current-thread
/// runtime, the same pattern as the session provider's `authenticate`.
///
/// Error mapping (Arm retries `Unavailable`, everything else is fatal):
/// - sealed (`503`)/unreachable/timeout → [`SecretError::Unavailable`]
/// - denied (`403`), absent (`404`), malformed → [`SecretError::Invalid`]
///
/// All secret material (JWT, client token, payload) lives in
/// [`SecretString`]/[`Zeroizing`] — nothing lands in logs or config.
pub struct VaultSecretResolver {
    addr: String,
    jwt_role: String,
    token_file: PathBuf,
    mount: String,
    client: reqwest::Client,
    token: std::sync::Mutex<Option<VaultToken>>,
}

/// Cached Vault client token. `renew_at` is when it should be renewed —
/// a slice of the granted TTL rather than the deadline itself, so a
/// transient renewal failure still leaves runway to retry or re-login.
struct VaultToken {
    value: SecretString,
    renew_at: std::time::Instant,
    valid_until: std::time::Instant,
}

const VAULT_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
const VAULT_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
/// Renew at this fraction of the granted lease.
const RENEW_FRACTION: f32 = 0.7;

impl VaultSecretResolver {
    pub fn new(addr: String, jwt_role: String, token_file: PathBuf, mount: Option<String>) -> Self {
        let client = reqwest::Client::builder()
            .connect_timeout(VAULT_CONNECT_TIMEOUT)
            .timeout(VAULT_REQUEST_TIMEOUT)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            addr: addr.trim_end_matches('/').to_owned(),
            jwt_role,
            token_file,
            mount: mount.unwrap_or_else(|| "kubernetes".into()),
            client,
            token: std::sync::Mutex::new(None),
        }
    }

    fn get(&self, path: &str) -> Result<SecretString, SecretError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| SecretError::Unavailable(format!("vault runtime unavailable: {e}")))?;
        runtime.block_on(self.get_async(path))
    }

    async fn get_async(&self, path: &str) -> Result<SecretString, SecretError> {
        let token = self.vault_token().await?;
        let url = format!("{}/v1/{}", self.addr, path.trim_start_matches('/'));
        let response = self
            .client
            .get(&url)
            .header("X-Vault-Token", token.reveal())
            .send()
            .await
            .map_err(|e| SecretError::Unavailable(format!("vault unreachable: {e}")))?;
        match response.status() {
            reqwest::StatusCode::OK => {}
            reqwest::StatusCode::FORBIDDEN => {
                return Err(SecretError::Invalid(format!("vault denied read of {path}")));
            }
            reqwest::StatusCode::NOT_FOUND => {
                return Err(SecretError::Invalid(format!(
                    "vault secret {path} not found"
                )));
            }
            status if status.is_server_error() => {
                return Err(SecretError::Unavailable(format!(
                    "vault is sealed or unavailable ({status})"
                )));
            }
            status => {
                return Err(SecretError::Unavailable(format!(
                    "vault read of {path} failed ({status})"
                )));
            }
        }
        let body: serde_json::Value = response
            .json()
            .await
            .map_err(|e| SecretError::Invalid(format!("vault returned malformed KV: {e}")))?;
        // KV v2: {"data": {"data": {<secret>}, "metadata": {…}}}. The
        // secret itself is re-serialized so callers see the same JSON
        // payload shape a file ref would carry.
        let data = body
            .pointer("/data/data")
            .cloned()
            .ok_or_else(|| SecretError::Invalid(format!("vault secret {path} has no data")))?;
        let payload = serde_json::to_string(&data)
            .map_err(|e| SecretError::Invalid(format!("vault secret {path} unencodable: {e}")))?;
        if payload.len() as u64 > MAX_SECRET_BYTES {
            return Err(SecretError::Invalid(format!(
                "vault secret {path} exceeds {MAX_SECRET_BYTES} bytes"
            )));
        }
        Ok(SecretString::new(payload))
    }

    /// A valid client token: cached → renew → re-login, in that order.
    async fn vault_token(&self) -> Result<SecretString, SecretError> {
        let cached = self.token.lock().unwrap().clone();
        let now = std::time::Instant::now();
        match cached {
            // Valid and comfortably inside its lease.
            Some(t) if now < t.renew_at => Ok(t.value.clone()),
            // Renewable window — try renew-self, fall back to login.
            Some(cached) if now < cached.valid_until => {
                if let Ok(fresh) = self.renew(&cached).await {
                    Ok(fresh.value.clone())
                } else {
                    self.login().await
                }
            }
            _ => self.login().await,
        }
    }

    /// `POST /v1/auth/<mount>/login` — JWT/k8s auth with the projected
    /// service-account token (re-read each login; the file rotates).
    async fn login(&self) -> Result<SecretString, SecretError> {
        let jwt = std::fs::read_to_string(&self.token_file).map_err(|e| {
            SecretError::Unavailable(format!(
                "vault service-account token {} is unreadable: {e}",
                self.token_file.display()
            ))
        })?;
        let url = format!("{}/v1/auth/{}/login", self.addr, self.mount);
        let response = self
            .client
            .post(&url)
            .json(&serde_json::json!({"jwt": jwt.trim(), "role": self.jwt_role}))
            .send()
            .await
            .map_err(|e| SecretError::Unavailable(format!("vault auth unreachable: {e}")))?;
        match response.status() {
            reqwest::StatusCode::OK => {}
            reqwest::StatusCode::FORBIDDEN | reqwest::StatusCode::BAD_REQUEST => {
                return Err(SecretError::Invalid(
                    "vault rejected the service-account login".into(),
                ));
            }
            status => {
                return Err(SecretError::Unavailable(format!(
                    "vault login failed ({status})"
                )));
            }
        }
        let body: serde_json::Value = response.json().await.map_err(|e| {
            SecretError::Unavailable(format!("vault login returned malformed body: {e}"))
        })?;
        let client_token = body
            .pointer("/auth/client_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                SecretError::Unavailable("vault login returned no client_token".into())
            })?;
        let lease = body
            .pointer("/auth/lease_duration")
            .and_then(|v| v.as_u64())
            .unwrap_or(3600);
        let value = SecretString::new(client_token);
        let now = std::time::Instant::now();
        let ttl = std::time::Duration::from_secs(lease.max(1));
        let fresh = VaultToken {
            value: value.clone(),
            renew_at: now + ttl.mul_f32(RENEW_FRACTION),
            valid_until: now + ttl,
        };
        *self.token.lock().unwrap() = Some(fresh);
        Ok(value)
    }

    /// `POST /v1/auth/token/renew-self` — extends the cached lease.
    async fn renew(&self, cached: &VaultToken) -> Result<VaultToken, SecretError> {
        let url = format!("{}/v1/auth/token/renew-self", self.addr);
        let response = self
            .client
            .post(&url)
            .header("X-Vault-Token", cached.value.reveal())
            .send()
            .await
            .map_err(|e| SecretError::Unavailable(format!("vault renew unreachable: {e}")))?;
        if !response.status().is_success() {
            return Err(SecretError::Unavailable(
                "vault token renewal refused".into(),
            ));
        }
        let body: serde_json::Value = response.json().await.unwrap_or_default();
        let lease = body
            .pointer("/auth/lease_duration")
            .and_then(|v| v.as_u64())
            .unwrap_or(3600);
        let now = std::time::Instant::now();
        let ttl = std::time::Duration::from_secs(lease.max(1));
        let fresh = VaultToken {
            value: cached.value.clone(),
            renew_at: now + ttl.mul_f32(RENEW_FRACTION),
            valid_until: now + ttl,
        };
        *self.token.lock().unwrap() = Some(fresh.clone());
        Ok(fresh)
    }
}

impl Clone for VaultToken {
    fn clone(&self) -> Self {
        VaultToken {
            value: self.value.clone(),
            renew_at: self.renew_at,
            valid_until: self.valid_until,
        }
    }
}

impl SecretResolver for VaultSecretResolver {
    fn resolve(&self, reference: &SecretRef) -> Result<SecretString, SecretError> {
        match reference {
            SecretRef::File(path) => FileSecretResolver::read(path),
            SecretRef::Vault { path } => self.get(path),
        }
    }
}

/// Build the resolver for a config: file-backed, plus Vault once supported.
pub fn resolver_for(config: &crate::config::BotdConfig) -> Arc<dyn SecretResolver> {
    match &config.vault {
        Some(vault) => Arc::new(VaultSecretResolver::new(
            vault.addr.clone(),
            vault.auth.jwt_role.clone(),
            vault.auth.token_file.clone(),
            vault.auth.mount.clone(),
        )),
        None => Arc::new(FileSecretResolver),
    }
}

// ---------------------------------------------------------------------
// Secret-file payload formats
// ---------------------------------------------------------------------

/// Parsed `account.credentials` content.
#[derive(Clone)]
pub enum AccountCredentials {
    Password {
        username: String,
        password: SecretString,
    },
    Session {
        dosid: SecretString,
        /// Optional instance override inside the credential file.
        server: Option<String>,
    },
}

impl fmt::Debug for AccountCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Password { username, .. } => f
                .debug_struct("Password")
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
            Self::Session { server, .. } => f
                .debug_struct("Session")
                .field("dosid", &"<redacted>")
                .field("server", server)
                .finish(),
        }
    }
}

fn invalid(message: impl Into<String>) -> SecretError {
    SecretError::Invalid(message.into())
}

/// Parse a raw secret payload: JSON object, `KEY=value` lines, or plain text.
fn parse_object(raw: &SecretString) -> Option<serde_json::Map<String, serde_json::Value>> {
    let text = raw.reveal().trim();
    if let Ok(serde_json::Value::Object(map)) = serde_json::from_str::<serde_json::Value>(text) {
        return Some(map);
    }
    // KEY=value lines (.env style); values may be quoted.
    let mut map = serde_json::Map::new();
    let mut any = false;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (key, value) = line.split_once('=')?;
        let key = key.trim();
        if key.is_empty() || !key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
            return None;
        }
        let value = value.trim().trim_matches('"').trim_matches('\'');
        map.insert(
            key.to_ascii_uppercase(),
            serde_json::Value::String(value.into()),
        );
        any = true;
    }
    any.then_some(map)
}

fn field(map: &serde_json::Map<String, serde_json::Value>, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        map.get(*key)
            .or_else(|| map.get(&key.to_ascii_uppercase()))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
    })
}

/// Interpret `account.credentials` content per `account.auth`.
///
/// Accepted shapes:
/// - password: `{"username": "…", "password": "…"}`, `USERNAME=…/PASSWORD=…`
///   lines, or plain `user:pass`.
/// - session: `{"dosid": "…", "server"?}`, `DOSID=…` lines, or a raw dosid.
pub fn parse_account_credentials(
    raw: &SecretString,
    auth: AuthMode,
) -> Result<AccountCredentials, SecretError> {
    let object = parse_object(raw);
    match auth {
        AuthMode::Password => {
            if let Some(map) = &object {
                let username = field(map, &["username", "user", "email"]);
                let password = field(map, &["password", "pass"]);
                if let (Some(username), Some(password)) = (username, password) {
                    return Ok(AccountCredentials::Password {
                        username,
                        password: SecretString::new(password),
                    });
                }
                return Err(invalid(
                    "credentials file must contain username and password",
                ));
            }
            let text = raw.reveal().trim();
            let Some((username, password)) = text.split_once(':') else {
                return Err(invalid(
                    "password credentials must be JSON, KEY=value lines, or user:pass",
                ));
            };
            if username.trim().is_empty() || password.is_empty() {
                return Err(invalid("user:pass credentials are incomplete"));
            }
            Ok(AccountCredentials::Password {
                username: username.trim().to_owned(),
                password: SecretString::new(password),
            })
        }
        AuthMode::Session => {
            if let Some(map) = &object {
                let dosid = field(map, &["dosid", "sid", "session_id"])
                    .ok_or_else(|| invalid("session credentials file must contain dosid"))?;
                return Ok(AccountCredentials::Session {
                    dosid: SecretString::new(dosid),
                    server: field(map, &["server", "instance"]),
                });
            }
            let dosid = raw.reveal().trim();
            if dosid.is_empty() || dosid.chars().any(char::is_whitespace) {
                return Err(invalid("session credentials must be a single dosid value"));
            }
            Ok(AccountCredentials::Session {
                dosid: SecretString::new(dosid),
                server: None,
            })
        }
    }
}

/// Parse a single-token secret (panel instance token, control token):
/// `{"token": "…"}`/`{"value": "…"}`, `TOKEN=…`, or plain text.
pub fn parse_token(raw: &SecretString, what: &str) -> Result<SecretString, SecretError> {
    if let Some(map) = parse_object(raw) {
        return field(&map, &["token", "value", "instance_token"])
            .map(SecretString::new)
            .ok_or_else(|| invalid(format!("{what} file must contain a token value")));
    }
    let text = raw.reveal().trim();
    if text.is_empty() || text.chars().any(char::is_whitespace) {
        return Err(invalid(format!("{what} must be a single token value")));
    }
    Ok(SecretString::new(text))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_secrets() {
        let secret = SecretString::new("hunter2");
        let dbg = format!("{secret:?}");
        assert!(!dbg.contains("hunter2"));
        assert!(dbg.contains("redacted"));
        assert_eq!(format!("{secret}"), "<redacted>");
    }

    #[test]
    fn credentials_debug_redacts() {
        let creds = AccountCredentials::Password {
            username: "pilot".into(),
            password: SecretString::new("hunter2"),
        };
        let dbg = format!("{creds:?}");
        assert!(dbg.contains("pilot"));
        assert!(!dbg.contains("hunter2"));
    }

    #[test]
    fn file_resolver_reads_and_rejects_vault() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token");
        std::fs::write(&path, "  abc123  ").unwrap();
        let resolver = FileSecretResolver;
        assert_eq!(
            resolver
                .resolve(&SecretRef::File(path.clone()))
                .unwrap()
                .reveal(),
            "  abc123  "
        );
        assert!(matches!(
            resolver.resolve(&SecretRef::File(dir.path().join("missing"))),
            Err(SecretError::Unavailable(_))
        ));
        assert!(matches!(
            resolver.resolve(&SecretRef::Vault {
                path: "kv/x".into()
            }),
            Err(SecretError::Invalid(_))
        ));
    }

    #[test]
    fn vault_resolver_reports_unreachable() {
        // No listener on this port → the login attempt maps to Unavailable
        // (transient — Arm retries), never a misleading Invalid.
        let resolver = VaultSecretResolver::new(
            "http://127.0.0.1:1".into(),
            "role".into(),
            "token".into(),
            None,
        );
        match resolver.resolve(&SecretRef::Vault {
            path: "kv/x".into(),
        }) {
            Err(SecretError::Unavailable(_)) => {}
            other => panic!("expected unavailable error, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------
    // In-process fake Vault (docs/09 §9.2): axum server mimicking the KV
    // v2 + JWT/k8s login + renew-self endpoints, incl. sealed (503).
    // -----------------------------------------------------------------

    mod fake_vault {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

        use axum::extract::{Path, State};
        use axum::http::StatusCode;
        use axum::routing::{get, post};
        use axum::{Json, Router};
        use serde_json::{Value, json};

        #[derive(Default)]
        pub struct State_ {
            /// Vault sealed — every endpoint answers 503.
            pub sealed: AtomicBool,
            pub logins: AtomicU64,
            pub reads: AtomicU64,
            pub renews: AtomicU64,
            /// JWT the login endpoint expects (checked, not echoed).
            pub jwt: std::sync::Mutex<String>,
            /// KV payload returned under `/v1/secret/data/*`.
            pub kv: std::sync::Mutex<Value>,
        }

        pub struct FakeVault {
            pub addr: String,
            pub state: Arc<State_>,
            /// Keeps the serving task alive.
            _runtime: tokio::runtime::Runtime,
        }

        impl FakeVault {
            pub fn start() -> Self {
                let state = Arc::new(State_::default());
                *state.jwt.lock().unwrap() = "sa-jwt".into();
                *state.kv.lock().unwrap() = json!({"dosid": "dosid-xyz"});
                let app = Router::new()
                    .route("/v1/auth/{mount}/login", post(login))
                    .route("/v1/auth/token/renew-self", post(renew))
                    .route("/v1/{*path}", get(read))
                    .with_state(state.clone());
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .enable_all()
                    .build()
                    .unwrap();
                let listener = runtime
                    .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
                    .unwrap();
                let addr = format!("http://{}", listener.local_addr().unwrap());
                runtime.spawn(async move {
                    axum::serve(listener, app).await.ok();
                });
                Self {
                    addr,
                    state,
                    _runtime: runtime,
                }
            }
        }

        async fn login(
            State(state): State<Arc<State_>>,
            Path(_mount): Path<String>,
            Json(body): Json<Value>,
        ) -> (StatusCode, Json<Value>) {
            if state.sealed.load(Ordering::Relaxed) {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"errors": ["sealed"]})),
                );
            }
            state.logins.fetch_add(1, Ordering::Relaxed);
            let expected = state.jwt.lock().unwrap().clone();
            if body.get("jwt").and_then(Value::as_str) != Some(expected.as_str()) {
                return (StatusCode::FORBIDDEN, Json(json!({"errors": ["bad jwt"]})));
            }
            (
                StatusCode::OK,
                Json(json!({
                    "auth": {
                        "client_token": "vault-client-token",
                        "lease_duration": 120,
                        "renewable": true
                    }
                })),
            )
        }

        async fn renew(State(state): State<Arc<State_>>) -> (StatusCode, Json<Value>) {
            if state.sealed.load(Ordering::Relaxed) {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"errors": ["sealed"]})),
                );
            }
            state.renews.fetch_add(1, Ordering::Relaxed);
            (
                StatusCode::OK,
                Json(json!({"auth": {"lease_duration": 120, "renewable": true}})),
            )
        }

        async fn read(
            State(state): State<Arc<State_>>,
            Path(_path): Path<String>,
            headers: axum::http::HeaderMap,
        ) -> (StatusCode, Json<Value>) {
            if state.sealed.load(Ordering::Relaxed) {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"errors": ["sealed"]})),
                );
            }
            state.reads.fetch_add(1, Ordering::Relaxed);
            if headers.get("x-vault-token").is_none() {
                return (
                    StatusCode::FORBIDDEN,
                    Json(json!({"errors": ["missing token"]})),
                );
            }
            let kv = state.kv.lock().unwrap().clone();
            (
                StatusCode::OK,
                Json(json!({"data": {"data": kv, "metadata": {}}})),
            )
        }
    }

    /// A resolver pointed at `fake` with a projected SA token file.
    fn vault_resolver(
        vault: &fake_vault::FakeVault,
        dir: &tempfile::TempDir,
    ) -> VaultSecretResolver {
        std::fs::write(dir.path().join("sa-token"), "sa-jwt\n").unwrap();
        VaultSecretResolver::new(
            vault.addr.clone(),
            "bot-role".into(),
            dir.path().join("sa-token"),
            None,
        )
    }

    #[test]
    fn vault_logs_in_and_reads_kv_v2() {
        let vault = fake_vault::FakeVault::start();
        let dir = tempfile::tempdir().unwrap();
        let resolver = vault_resolver(&vault, &dir);
        let secret = resolver
            .resolve(&SecretRef::Vault {
                path: "secret/data/bots/bot-1/account".into(),
            })
            .unwrap();
        assert_eq!(secret.reveal(), r#"{"dosid":"dosid-xyz"}"#);
        assert_eq!(
            vault
                .state
                .logins
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        // File refs still work through the composite resolver.
        let file = dir.path().join("plain");
        std::fs::write(&file, "tok").unwrap();
        assert_eq!(
            resolver.resolve(&SecretRef::File(file)).unwrap().reveal(),
            "tok"
        );
    }

    #[test]
    fn vault_sealed_is_transient() {
        let vault = fake_vault::FakeVault::start();
        let dir = tempfile::tempdir().unwrap();
        let resolver = vault_resolver(&vault, &dir);
        vault
            .state
            .sealed
            .store(true, std::sync::atomic::Ordering::Relaxed);
        match resolver.resolve(&SecretRef::Vault {
            path: "secret/data/x".into(),
        }) {
            Err(error) => assert!(error.is_transient(), "sealed must be transient: {error}"),
            other => panic!("sealed vault must fail, got {other:?}"),
        }
        // …and the pod recovers without a restart once unsealed.
        vault
            .state
            .sealed
            .store(false, std::sync::atomic::Ordering::Relaxed);
        assert!(
            resolver
                .resolve(&SecretRef::Vault {
                    path: "secret/data/x".into()
                })
                .is_ok()
        );
    }

    #[test]
    fn vault_denied_is_invalid() {
        let vault = fake_vault::FakeVault::start();
        let dir = tempfile::tempdir().unwrap();
        // Wrong SA token → login 403 → Invalid (permanent — fix the role).
        std::fs::write(dir.path().join("sa-token"), "wrong-jwt").unwrap();
        let resolver = VaultSecretResolver::new(
            vault.addr.clone(),
            "bot-role".into(),
            dir.path().join("sa-token"),
            None,
        );
        match resolver.resolve(&SecretRef::Vault {
            path: "secret/data/x".into(),
        }) {
            Err(SecretError::Invalid(_)) => {}
            other => panic!("denied login must be Invalid, got {other:?}"),
        }
    }

    #[test]
    fn parses_password_credentials() {
        for raw in [
            r#"{"username":"u","password":"p"}"#,
            "USERNAME=u\nPASSWORD=p\n",
            "u:p",
        ] {
            match parse_account_credentials(&SecretString::new(raw), AuthMode::Password) {
                Ok(AccountCredentials::Password { username, password }) => {
                    assert_eq!(username, "u");
                    assert_eq!(password.reveal(), "p");
                }
                other => panic!("{raw}: {other:?}"),
            }
        }
        assert!(
            parse_account_credentials(&SecretString::new("just-a-user"), AuthMode::Password)
                .is_err()
        );
    }

    #[test]
    fn parses_session_credentials() {
        match parse_account_credentials(
            &SecretString::new(r#"{"dosid":"abc","server":"int2"}"#),
            AuthMode::Session,
        ) {
            Ok(AccountCredentials::Session { dosid, server }) => {
                assert_eq!(dosid.reveal(), "abc");
                assert_eq!(server.as_deref(), Some("int2"));
            }
            other => panic!("{other:?}"),
        }
        match parse_account_credentials(&SecretString::new("rawdosid123"), AuthMode::Session) {
            Ok(AccountCredentials::Session { dosid, server }) => {
                assert_eq!(dosid.reveal(), "rawdosid123");
                assert!(server.is_none());
            }
            other => panic!("{other:?}"),
        }
        assert!(
            parse_account_credentials(&SecretString::new("has space"), AuthMode::Session).is_err()
        );
    }

    #[test]
    fn parses_tokens() {
        assert_eq!(
            parse_token(&SecretString::new(r#"{"token":"t1"}"#), "panel")
                .unwrap()
                .reveal(),
            "t1"
        );
        assert_eq!(
            parse_token(&SecretString::new("plain-token"), "panel")
                .unwrap()
                .reveal(),
            "plain-token"
        );
        assert!(parse_token(&SecretString::new("two words"), "panel").is_err());
    }
}
