//! `botd.json` — schema version 1 (orchestrator `docs/02-headless-bot.md` §2.2).
//!
//! The config never carries secrets inline: every secret is a
//! [`crate::secrets::SecretRef`]. `settings` embeds the exact
//! `BotSettings`/`modules.json` shape the GUI persists.
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use schemars::JsonSchema;
use serde::Deserialize;
use xenomit_bot_runtime::{BotSettings, ClientKind};

use crate::secrets::SecretRef;

/// The only `schema` value this binary accepts.
pub const SCHEMA_VERSION: u32 = 1;
/// `XENOMIT_BOTD_CONFIG` — second precedence after `--config`.
pub const ENV_CONFIG: &str = "XENOMIT_BOTD_CONFIG";
/// Last-resort config path.
pub const DEFAULT_CONFIG_PATH: &str = "/etc/xenomit/botd.json";
/// Hard size cap per the security doc's input limits (`docs/09-security.md` §9.1).
const MAX_CONFIG_BYTES: u64 = 128 * 1024;

fn default_health_bind() -> String {
    "127.0.0.1:8080".into()
}

fn default_state_dir() -> PathBuf {
    PathBuf::from("./botd-state")
}

fn default_vault_token_file() -> PathBuf {
    PathBuf::from("/var/run/secrets/vault/token")
}

/// Root `botd.json` document.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BotdConfig {
    /// Config schema version; must be `1`.
    pub schema: u32,
    /// Instance identity assigned by the orchestrator.
    pub bot: BotIdentity,
    /// Game account: server, auth mode, credential reference.
    pub account: AccountConfig,
    /// Game client selection: `"packet"` or `"flash"`.
    pub client: ClientConfig,
    /// Module/behavior settings — the exact `modules.json` (`BotSettings`)
    /// shape the GUI persists. `{}`/absent uses `BotSettings::default()`.
    #[serde(default)]
    pub settings: serde_json::Value,
    /// Start `settings.active_module` immediately after attach.
    #[serde(default)]
    pub autostart: bool,
    /// Egress networking.
    #[serde(default)]
    pub network: NetworkConfig,
    /// Managed mode: orchestrator control-plane link. Absent = standalone
    /// single-process operation (phase 1a).
    #[serde(default)]
    pub control: Option<ControlConfig>,
    /// Web panel link (Agent Protocol v1 over WebSocket).
    #[serde(default)]
    pub panel: Option<PanelConfig>,
    /// Vault backend used by `vault` secret references.
    #[serde(default)]
    pub vault: Option<VaultConfig>,
    /// Graceful-handoff budgets.
    #[serde(default)]
    pub handoff: HandoffConfig,
    /// Telemetry export (OTLP).
    #[serde(default)]
    pub telemetry: TelemetryConfig,
    /// Local state directory: statistics journals, the stat outbox and later
    /// handoff snapshots live under here.
    #[serde(default = "default_state_dir")]
    pub state_dir: PathBuf,
    /// Local health endpoints.
    #[serde(default)]
    pub health: HealthConfig,
    /// Test-only endpoint overrides for the mock game server. Must stay
    /// absent in production configs.
    #[serde(default)]
    pub testing: Option<TestingConfig>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BotIdentity {
    /// Stable bot identifier (e.g. `bot_01h…`); survives restarts.
    pub id: String,
    /// Owning tenant/user.
    pub tenant: String,
    /// Optional display label.
    #[serde(default)]
    pub label: Option<String>,
}

/// `account.auth` — which credential shape the credentials file holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuthMode {
    /// `{"username", "password"}` (or `user:pass`) → Backpage password login.
    Password,
    /// `{"dosid"}` (or a raw dosid) → reuse an issued session id.
    Session,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AccountConfig {
    /// Backpage instance, e.g. `int1.darkorbit.com` (`int1` also accepted).
    pub server: String,
    /// Credential shape expected from `credentials`.
    pub auth: AuthMode,
    /// Where the credentials live — never inline.
    pub credentials: SecretRef,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClientConfig {
    /// `"packet"` or `"flash"` (Linux Pepper host with a virtual display).
    pub kind: String,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct NetworkConfig {
    /// Egress proxy definition secret: `{"scheme","host","port","username",
    /// "password"}` or `{"url": "socks5://user:pass@host:1080"}`.
    /// The operator renders `{"mode": "fixed|pool", "vault": {"path": …}}`
    /// — the policy key is ignored here; the pool/fixed decision is the
    /// controller's, the pod only sees the resolved secret reference.
    #[serde(default, deserialize_with = "deserialize_proxy_ref")]
    pub proxy: Option<SecretRef>,
    /// Refuse to arm when no usable proxy is configured (exit 69).
    pub require_proxy: bool,
}

/// `network.proxy` accepts either the canonical `{"vault"|"file": …}`
/// shape or the operator's rendered `{"mode": …, "vault"|"file": …}`.
fn deserialize_proxy_ref<'de, D>(de: D) -> Result<Option<SecretRef>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let value = Option::<serde_json::Value>::deserialize(de)?;
    let Some(mut value) = value else {
        return Ok(None);
    };
    // Unwrap the operator's `{"mode": "…", <ref>}` envelope; the secret
    // reference itself is the remaining single-keyed object.
    if let Some(mode) = value.get("mode").and_then(|v| v.as_str()) {
        if !matches!(mode, "fixed" | "pool") {
            return Err(D::Error::custom(format!(
                "network.proxy.mode must be \"fixed\" or \"pool\" (got \"{mode}\")"
            )));
        }
        if let Some(obj) = value.as_object_mut() {
            obj.remove("mode");
        }
    }
    serde_json::from_value(value)
        .map(Some)
        .map_err(|e| D::Error::custom(format!("network.proxy: {e}")))
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ControlConfig {
    /// Orchestrator gateway base URL for the control stream (`http(s)://`).
    /// The operator renders no endpoint — pods resolve it from the
    /// `XENOMIT_CONTROL_ENDPOINT` env var the backend injects.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Service-account token presented to the control stream.
    pub token: SecretRef,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PanelConfig {
    /// Panel base URL (`http(s)://…` or `ws(s)://…`; a bare host means wss).
    /// Pods without panel pairing leave it unset — the link stays disabled.
    #[serde(default)]
    pub base_url: Option<String>,
    /// Permanent instance token minted by panel pairing.
    pub instance_token: SecretRef,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct VaultConfig {
    /// Vault base URL, e.g. `https://vault.example.internal:8200`.
    pub addr: String,
    /// How the bot authenticates to Vault.
    pub auth: VaultAuth,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct VaultAuth {
    /// Vault JWT role bound to this bot's service account.
    pub jwt_role: String,
    /// Projected service-account token file.
    pub token_file: PathBuf,
    /// Vault auth-mount for the JWT/k8s login (`auth/<mount>/login`).
    /// Defaults to `kubernetes`.
    #[serde(default)]
    pub mount: Option<String>,
}

/// `handoff.restore_policy` — what a bad or missing snapshot means at
/// Activate (docs/03 §3.5).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RestorePolicy {
    /// Restore what parses; on corruption log a warning and start fresh.
    #[default]
    BestEffort,
    /// A clean snapshot that cannot be restored aborts the pod (exit 70)
    /// instead of silently starting at spawn.
    Strict,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct HandoffConfig {
    /// Graceful drain budget — SIGTERM to clean disconnect.
    pub drain_deadline_s: u64,
    /// Hard abort ceiling; the supervisor escalates past this.
    pub hard_deadline_s: u64,
    /// Periodic checkpoint cadence while running.
    pub checkpoint_interval_s: u64,
    /// Snapshot restore policy at Activate.
    pub restore_policy: RestorePolicy,
}

impl Default for HandoffConfig {
    fn default() -> Self {
        Self {
            drain_deadline_s: 600,
            hard_deadline_s: 1800,
            checkpoint_interval_s: 15,
            restore_policy: RestorePolicy::BestEffort,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct TelemetryConfig {
    /// OTLP collector endpoint; `null` falls back to
    /// `OTEL_EXPORTER_OTLP_ENDPOINT`, unset disables export.
    pub otlp_endpoint: Option<String>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct HealthConfig {
    /// `host:port` for `/startupz` `/readyz` `/livez` (localhost only).
    pub bind: String,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            bind: default_health_bind(),
        }
    }
}

/// Endpoint overrides that let tests target a mock game server instead of
/// the real DarkOrbit infrastructure.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct TestingConfig {
    /// Base URL replacing `https://<account.server>/` for every Backpage
    /// call, e.g. `http://127.0.0.1:8081`.
    pub backpage_base_url: Option<String>,
    /// Frontpage base URL for password login; defaults to
    /// `backpage_base_url` then `https://www.darkorbit.com/`.
    pub front_base_url: Option<String>,
    /// `host:port` game endpoint replacing the `maps.php` lookup.
    pub game_endpoint: Option<String>,
}

/// Config could not be read, parsed or validated.
#[derive(Debug)]
pub enum ConfigError {
    Io { path: PathBuf, error: String },
    Parse { path: PathBuf, error: String },
    Validation { path: PathBuf, errors: Vec<String> },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { path, error } => write!(f, "Could not read {}: {error}", path.display()),
            Self::Parse { path, error } => {
                write!(f, "Could not parse {}: {error}", path.display())
            }
            Self::Validation { path, errors } => {
                writeln!(f, "Invalid {}:", path.display())?;
                for error in errors {
                    writeln!(f, "  - {error}")?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for ConfigError {}

/// `--config` → `$XENOMIT_BOTD_CONFIG` → `/etc/xenomit/botd.json`.
pub fn resolve_path(flag: Option<PathBuf>) -> PathBuf {
    flag.or_else(|| std::env::var_os(ENV_CONFIG).map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH))
}

/// Generated JSON Schema for [`BotdConfig`] — served by `--print-schema`
/// and committed to `schema/botd.schema.json`.
pub fn json_schema() -> String {
    let schema = schemars::schema_for!(BotdConfig);
    serde_json::to_string_pretty(&schema).expect("schema serializes")
}

fn uses_vault(reference: &SecretRef) -> bool {
    matches!(reference, SecretRef::Vault { .. })
}

fn is_http_url(value: &str) -> bool {
    let lower = value.trim_start().to_ascii_lowercase();
    lower.starts_with("http://") || lower.starts_with("https://")
}

fn is_panel_url(value: &str) -> bool {
    let lower = value.trim_start().to_ascii_lowercase();
    lower.starts_with("http://")
        || lower.starts_with("https://")
        || lower.starts_with("ws://")
        || lower.starts_with("wss://")
}

fn is_host_port(value: &str) -> bool {
    if value.parse::<SocketAddr>().is_ok() {
        return true;
    }
    match value.rsplit_once(':') {
        Some((host, port)) => !host.trim().is_empty() && port.parse::<u16>().is_ok(),
        None => false,
    }
}

impl BotdConfig {
    /// Read (≤128 KiB), parse and validate a `botd.json`.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let io = |error: std::io::Error| ConfigError::Io {
            path: path.to_path_buf(),
            error: error.to_string(),
        };
        let metadata = std::fs::metadata(path).map_err(io)?;
        if metadata.len() > MAX_CONFIG_BYTES {
            return Err(ConfigError::Io {
                path: path.to_path_buf(),
                error: format!("config exceeds {} bytes", MAX_CONFIG_BYTES),
            });
        }
        let text = std::fs::read_to_string(path).map_err(io)?;
        let mut config: BotdConfig =
            serde_json::from_str(&text).map_err(|error| ConfigError::Parse {
                path: path.to_path_buf(),
                error: error.to_string(),
            })?;
        config.apply_env_overrides();
        if let Err(errors) = config.validate() {
            return Err(ConfigError::Validation {
                path: path.to_path_buf(),
                errors,
            });
        }
        Ok(config)
    }

    /// Fill fields the operator leaves for pod env vars. The controller's
    /// rendered config never carries deployment endpoints — the pod
    /// backend injects them (`XENOMIT_CONTROL_ENDPOINT`, `VAULT_ADDR`,
    /// `XENOMIT_STATE_DIR`, the `XENOMIT_TESTING_*` mock endpoints, …).
    /// Anything already set in the config file wins.
    fn apply_env_overrides(&mut self) {
        let env = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());

        if let Some(dir) = env("XENOMIT_STATE_DIR") {
            self.state_dir = PathBuf::from(dir);
        }
        if let Some(bind) = env("XENOMIT_HEALTH_BIND") {
            self.health.bind = bind;
        }
        // The pod backend stamps identity — the rendered config's `bot.id`
        // is a placeholder in managed mode.
        if let Some(id) = env("XENOMIT_BOT_ID") {
            self.bot.id = id;
        }
        // Managed mode: the control block itself may be env-synthesized
        // when a token file is also injected, but normally the rendered
        // config carries `control.token` and only the endpoint is missing.
        let control_env = env("XENOMIT_CONTROL_ENDPOINT");
        match self.control.as_mut() {
            Some(control) => {
                if control.endpoint.is_none()
                    && let Some(endpoint) = control_env
                {
                    control.endpoint = Some(endpoint);
                }
            }
            None => {
                if let (Some(endpoint), Some(token_file)) = (
                    control_env,
                    env("XENOMIT_CONTROL_TOKEN_FILE").map(PathBuf::from),
                ) {
                    self.control = Some(ControlConfig {
                        endpoint: Some(endpoint),
                        token: SecretRef::File(token_file),
                    });
                }
            }
        }
        if self.vault.is_none()
            && let Some(addr) = env("XENOMIT_VAULT_ADDR").or_else(|| env("VAULT_ADDR"))
        {
            self.vault = Some(VaultConfig {
                addr,
                auth: VaultAuth {
                    jwt_role: env("XENOMIT_VAULT_ROLE")
                        .unwrap_or_else(|| format!("bot-{}", self.bot.id)),
                    token_file: env("XENOMIT_VAULT_TOKEN_FILE")
                        .or_else(|| env("VAULT_TOKEN_FILE"))
                        .map(PathBuf::from)
                        .unwrap_or_else(default_vault_token_file),
                    mount: env("XENOMIT_VAULT_MOUNT"),
                },
            });
        }
        // Mock-game endpoints (e2e + local dev): only ever env-injected,
        // never rendered by the operator.
        let mut testing = self.testing.take().unwrap_or_default();
        let mut touched = false;
        for (field, name) in [
            (
                &mut testing.backpage_base_url,
                "XENOMIT_TESTING_BACKPAGE_URL",
            ),
            (&mut testing.front_base_url, "XENOMIT_TESTING_FRONT_URL"),
            (&mut testing.game_endpoint, "XENOMIT_TESTING_GAME_ENDPOINT"),
        ] {
            if field.is_none()
                && let Some(value) = env(name)
            {
                *field = Some(value);
                touched = true;
            }
        }
        if touched || self.testing.is_some() {
            self.testing = Some(testing);
        }
    }

    /// This pod's fencing epoch — the pod backend stamps `XENOMIT_EPOCH`
    /// on every spawn (docs/03 §3.7). `0` = unmanaged/standalone.
    pub fn pod_epoch(&self) -> u64 {
        std::env::var("XENOMIT_EPOCH")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    }

    /// Exit-IP echo URL override (e2e); `None` = the client's default.
    pub fn exit_ip_url(&self) -> Option<String> {
        std::env::var("XENOMIT_EXIT_IP_URL")
            .ok()
            .filter(|v| !v.is_empty())
    }

    /// The control-plane endpoint after env resolution (config wins).
    pub fn control_endpoint(&self) -> Option<String> {
        self.control
            .as_ref()
            .and_then(|c| c.endpoint.clone())
            .or_else(|| {
                std::env::var("XENOMIT_CONTROL_ENDPOINT")
                    .ok()
                    .filter(|v| !v.is_empty())
            })
    }

    /// All deployment-level checks that do not need secret material.
    /// Returns every violation at once so operators fix one pass.
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();
        let mut fail = |error: String| errors.push(error);

        if self.schema != SCHEMA_VERSION {
            fail(format!(
                "schema must be {SCHEMA_VERSION}; got {}",
                self.schema
            ));
        }
        if self.bot.id.trim().is_empty() {
            fail("bot.id is required".into());
        }
        if self.bot.tenant.trim().is_empty() {
            fail("bot.tenant is required".into());
        }
        if self.account.server.trim().is_empty() {
            fail("account.server is required".into());
        }
        if !matches!(self.client.kind.as_str(), "packet" | "flash") {
            fail(format!(
                "client.kind must be \"packet\" or \"flash\" (got \"{}\")",
                self.client.kind
            ));
        }
        if self.client.kind == "flash"
            && (self.network.proxy.is_some() || self.network.require_proxy)
        {
            fail("Flash does not support network.proxy; use packet or externally enforced container egress".into());
        }
        if self.client.kind == "flash" && !cfg!(target_os = "linux") {
            fail("botd Flash client requires Linux".into());
        }
        match self.bot_settings() {
            Ok(_) => {}
            Err(error) => fail(error),
        }
        if let Some(client) = self.settings.get("client").and_then(|v| v.as_str())
            && client != self.client.kind
        {
            fail(format!(
                "settings.client must match client.kind (got \"{client}\"); use client.kind"
            ));
        }
        if self.autostart && {
            let settings = self.bot_settings().unwrap_or_default();
            settings.active_module.trim().is_empty()
        } {
            fail("autostart requires settings.active_module".into());
        }
        if self.network.require_proxy && self.network.proxy.is_none() {
            fail("network.require_proxy is set but no network.proxy is configured".into());
        }
        let any_vault = uses_vault(&self.account.credentials)
            || self.network.proxy.as_ref().is_some_and(uses_vault)
            || self
                .panel
                .as_ref()
                .is_some_and(|p| uses_vault(&p.instance_token))
            || self.control.as_ref().is_some_and(|c| uses_vault(&c.token));
        if any_vault && self.vault.is_none() {
            fail("a secret references vault but no vault block is configured".into());
        }
        if let Some(panel) = &self.panel
            && let Some(base_url) = &panel.base_url
            && !is_panel_url(base_url)
        {
            fail(format!(
                "panel.base_url must be http(s):// or ws(s):// (got \"{base_url}\")"
            ));
        }
        if self.control.is_some() {
            match self.control_endpoint() {
                Some(endpoint) if is_http_url(&endpoint) => {}
                Some(endpoint) => fail(format!(
                    "control.endpoint must be http(s):// (got \"{endpoint}\")"
                )),
                None => {
                    fail("control.endpoint is required (config or XENOMIT_CONTROL_ENDPOINT)".into())
                }
            }
        }
        if let Some(vault) = &self.vault {
            if !is_http_url(&vault.addr) {
                fail(format!(
                    "vault.addr must be http(s):// (got \"{}\")",
                    vault.addr
                ));
            }
            if vault.auth.jwt_role.trim().is_empty() {
                fail("vault.auth.jwt_role is required".into());
            }
            if vault.auth.token_file.as_os_str().is_empty() {
                fail("vault.auth.token_file is required".into());
            }
        }
        if self.handoff.drain_deadline_s == 0 {
            fail("handoff.drain_deadline_s must be positive".into());
        }
        if self.handoff.hard_deadline_s < self.handoff.drain_deadline_s {
            fail("handoff.hard_deadline_s must be >= drain_deadline_s".into());
        }
        if self.handoff.checkpoint_interval_s == 0 {
            fail("handoff.checkpoint_interval_s must be positive".into());
        }
        if !is_host_port(&self.health.bind) {
            fail(format!(
                "health.bind must be host:port (got \"{}\")",
                self.health.bind
            ));
        }
        if let Some(telemetry) = &self.telemetry.otlp_endpoint
            && !is_http_url(telemetry)
        {
            fail(format!(
                "telemetry.otlp_endpoint must be http(s):// (got \"{telemetry}\")"
            ));
        }
        if let Some(testing) = &self.testing {
            for (name, value) in [
                ("testing.backpage_base_url", &testing.backpage_base_url),
                ("testing.front_base_url", &testing.front_base_url),
            ] {
                if let Some(value) = value
                    && !is_http_url(value)
                {
                    fail(format!("{name} must be http(s):// (got \"{value}\")"));
                }
            }
            if let Some(endpoint) = &testing.game_endpoint
                && !is_host_port(endpoint)
            {
                fail(format!(
                    "testing.game_endpoint must be host:port (got \"{endpoint}\")"
                ));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Parse `settings` into the shared `BotSettings` shape, forcing the
    /// configured client kind.
    pub fn bot_settings(&self) -> Result<BotSettings, String> {
        // `null` (explicit or absent) means the modules.json defaults.
        let raw = if self.settings.is_null() {
            serde_json::json!({})
        } else {
            self.settings.clone()
        };
        let mut settings: BotSettings = serde_json::from_value(raw).map_err(|error| {
            format!("settings must match the BotSettings (modules.json) shape: {error}")
        })?;
        settings.client = match self.client.kind.as_str() {
            "flash" => ClientKind::Flash,
            _ => ClientKind::Packet,
        };
        Ok(settings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn minimal() -> serde_json::Value {
        json!({
            "schema": 1,
            "bot": { "id": "bot_1", "tenant": "t1" },
            "account": {
                "server": "int1.darkorbit.com",
                "auth": "session",
                "credentials": { "file": "/run/secrets/account.json" }
            },
            "client": { "kind": "packet" }
        })
    }

    fn validate(value: serde_json::Value) -> Result<(), Vec<String>> {
        serde_json::from_value::<BotdConfig>(value)
            .map_err(|e| vec![format!("parse: {e}")])
            .and_then(|config| config.validate())
    }

    #[test]
    fn minimal_config_validates() {
        validate(minimal()).unwrap();
    }

    #[test]
    fn rejects_wrong_schema() {
        let mut value = minimal();
        value["schema"] = json!(2);
        let errors = validate(value).unwrap_err();
        assert!(errors.iter().any(|e| e.contains("schema")));
    }

    #[test]
    fn flash_settings_and_proxy_validation() {
        let mut value = minimal();
        value["client"]["kind"] = json!("flash");
        let config: BotdConfig = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(config.bot_settings().unwrap().client, ClientKind::Flash);
        if cfg!(target_os = "linux") {
            assert!(validate(value.clone()).is_ok());
        }
        value["network"] = json!({"proxy": {"file": "/run/secrets/proxy"}});
        assert!(
            validate(value)
                .unwrap_err()
                .iter()
                .any(|e| e.contains("Flash does not support network.proxy"))
        );
    }

    #[test]
    fn rejects_non_packet_client() {
        let mut value = minimal();
        value["client"]["kind"] = json!("ruffle");
        let errors = validate(value).unwrap_err();
        assert!(errors.iter().any(|e| e.contains("packet")));
    }

    #[test]
    fn rejects_unknown_fields() {
        let mut value = minimal();
        value["bogus"] = json!(true);
        assert!(serde_json::from_value::<BotdConfig>(value).is_err());
    }

    #[test]
    fn requires_identity_and_server() {
        let mut value = minimal();
        value["bot"]["id"] = json!("");
        value["bot"]["tenant"] = json!("  ");
        value["account"]["server"] = json!("");
        let errors = validate(value).unwrap_err();
        assert!(errors.iter().any(|e| e.contains("bot.id")));
        assert!(errors.iter().any(|e| e.contains("bot.tenant")));
        assert!(errors.iter().any(|e| e.contains("account.server")));
    }

    #[test]
    fn require_proxy_needs_proxy() {
        let mut value = minimal();
        value["network"] = json!({ "require_proxy": true });
        let errors = validate(value).unwrap_err();
        assert!(errors.iter().any(|e| e.contains("require_proxy")));
    }

    #[test]
    fn vault_ref_requires_vault_block() {
        let mut value = minimal();
        value["account"]["credentials"] = json!({ "vault": { "path": "bots/b1" } });
        let errors = validate(value).unwrap_err();
        assert!(errors.iter().any(|e| e.contains("vault")));
    }

    #[test]
    fn handoff_deadlines_checked() {
        let mut value = minimal();
        value["handoff"] = json!({ "drain_deadline_s": 900, "hard_deadline_s": 60 });
        let errors = validate(value).unwrap_err();
        assert!(errors.iter().any(|e| e.contains("hard_deadline")));
    }

    #[test]
    fn testing_endpoints_checked() {
        let mut value = minimal();
        value["testing"] = json!({
            "backpage_base_url": "not-a-url",
            "game_endpoint": "127.0.0.1:5001"
        });
        let errors = validate(value).unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| e.contains("testing.backpage_base_url"))
        );
        let mut value = minimal();
        value["testing"] = json!({ "game_endpoint": "no-port" });
        let errors = validate(value).unwrap_err();
        assert!(errors.iter().any(|e| e.contains("game_endpoint")));
    }

    #[test]
    fn settings_parse_forces_packet() {
        let mut value = minimal();
        value["settings"] = json!({ "active_module": "kill_collect", "client": "packet" });
        let config: BotdConfig = serde_json::from_value(value).unwrap();
        let settings = config.bot_settings().unwrap();
        assert_eq!(settings.client, ClientKind::Packet);
        assert_eq!(settings.active_module, "kill_collect");
    }

    #[test]
    fn settings_client_must_be_packet() {
        let mut value = minimal();
        value["settings"] = json!({ "client": "ruffle" });
        let errors = validate(value).unwrap_err();
        assert!(errors.iter().any(|e| e.contains("settings.client")));
    }

    #[test]
    fn autostart_needs_active_module() {
        let mut value = minimal();
        value["autostart"] = json!(true);
        value["settings"] = json!({ "active_module": "" });
        let errors = validate(value).unwrap_err();
        assert!(errors.iter().any(|e| e.contains("active_module")));
    }

    #[test]
    fn env_precedence() {
        // --config wins over everything; env beats the default path.
        let flag = PathBuf::from("/flag.json");
        assert_eq!(resolve_path(Some(flag.clone())), flag);
        unsafe { std::env::set_var(ENV_CONFIG, "/env.json") };
        assert_eq!(resolve_path(None), PathBuf::from("/env.json"));
        unsafe { std::env::remove_var(ENV_CONFIG) };
        assert_eq!(resolve_path(None), PathBuf::from(DEFAULT_CONFIG_PATH));
    }

    #[test]
    fn schema_generation_is_stable() {
        let schema = json_schema();
        assert!(schema.contains("\"BotdConfig\"") || schema.contains("botd"));
        let reparsed: serde_json::Value = serde_json::from_str(&schema).unwrap();
        assert!(reparsed.is_object());
    }
}
