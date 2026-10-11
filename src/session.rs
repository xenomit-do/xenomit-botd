//! [`SessionProvider`] for the daemon: resolves `account.credentials` at Arm
//! (validated early) and again per authentication attempt — file resolvers
//! re-read so rotated secrets apply; a momentarily missing file falls back
//! to the armed credentials for refresh re-login.
use std::sync::{Arc, Mutex};

use xenomit_backpage::{ProxyConfig, Session};
use xenomit_bot_runtime::SessionProvider;
use xenomit_core::logging::tracing::{info, warn};

use crate::config::{AccountConfig, NetworkConfig, TestingConfig};
use crate::error::BotdError;
use crate::secrets::{AccountCredentials, SecretError, SecretResolver, SecretString};

/// Config-backed provider. When `network.proxy` is set every Backpage
/// call routes through it — there is no direct fallback (docs/05 §5.4).
pub struct HeadlessSessionProvider {
    account: AccountConfig,
    testing: Option<TestingConfig>,
    resolver: Arc<dyn SecretResolver>,
    network: NetworkConfig,
    /// Exit-IP echo endpoint override (e2e); `None` = `api.ipify.org`.
    exit_ip_url: Option<String>,
    credentials: Mutex<Option<AccountCredentials>>,
    /// Armed proxy (resolved once at `Arm`; fail-closed until then).
    proxy: Mutex<Option<ProxyConfig>>,
    /// Observed exit IP after a proxied login.
    exit_ip: Mutex<Option<String>>,
}

impl HeadlessSessionProvider {
    pub fn new(
        account: AccountConfig,
        testing: Option<TestingConfig>,
        resolver: Arc<dyn SecretResolver>,
        network: NetworkConfig,
        exit_ip_url: Option<String>,
    ) -> Self {
        Self {
            account,
            testing,
            resolver,
            network,
            exit_ip_url,
            credentials: Mutex::new(None),
            proxy: Mutex::new(None),
            exit_ip: Mutex::new(None),
        }
    }

    /// Resolve and parse the credentials + proxy files; called once during
    /// `Arm` so bad secrets fail before any network traffic.
    pub fn arm(&self) -> Result<(), SecretError> {
        let credentials = self.resolve()?;
        if let Ok(mut slot) = self.credentials.lock() {
            *slot = Some(credentials);
        }
        if let Some(reference) = &self.network.proxy {
            let raw = self.resolver.resolve(reference)?;
            let spec = crate::proxy::parse_proxy(&raw)?;
            *self.proxy.lock().unwrap_or_else(|e| e.into_inner()) = Some(spec.to_backpage_config());
        }
        Ok(())
    }

    /// The proxy's observed exit IP — set after a proxied `validate`.
    pub fn exit_ip(&self) -> Option<String> {
        self.exit_ip
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// The armed proxy spec for the packet attach, resolved fresh so a
    /// rotated secret applies. `None` when no `network.proxy` is set.
    pub fn packet_proxy(
        &self,
    ) -> Result<Option<xenomit_packet_client::proxy::ProxyConfig>, SecretError> {
        match &self.network.proxy {
            Some(reference) => {
                let raw = self.resolver.resolve(reference)?;
                let spec = crate::proxy::parse_proxy(&raw)?;
                Ok(Some(spec.to_packet_config()))
            }
            None => Ok(None),
        }
    }

    fn resolve(&self) -> Result<AccountCredentials, SecretError> {
        let raw = self.resolver.resolve(&self.account.credentials)?;
        crate::secrets::parse_account_credentials(&raw, self.account.auth)
    }

    fn credentials(&self) -> Result<AccountCredentials, BotdError> {
        match self.resolve() {
            Ok(credentials) => {
                if let Ok(mut slot) = self.credentials.lock() {
                    *slot = Some(credentials.clone());
                }
                Ok(credentials)
            }
            Err(error) if error.is_transient() => self
                .credentials
                .lock()
                .ok()
                .and_then(|slot| slot.clone())
                .ok_or_else(|| BotdError::Unavailable(format!("credentials unresolved: {error}"))),
            Err(SecretError::Unavailable(message)) | Err(SecretError::Invalid(message)) => {
                Err(BotdError::Config(message))
            }
        }
    }

    /// Authenticate via Backpage. Blocking: builds a short-lived
    /// current-thread runtime internally, like `BackpageSessionProvider`.
    pub fn authenticate(&self) -> Result<Session, BotdError> {
        let credentials = self.credentials()?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| {
                BotdError::Fatal(format!("could not create login runtime: {error}"))
            })?;
        runtime.block_on(async {
            match credentials {
                AccountCredentials::Password { username, password } => {
                    self.password_login(&username, &password).await
                }
                AccountCredentials::Session { dosid, server } => {
                    self.sid_login(&dosid, server.as_deref()).await
                }
            }
        })
    }

    fn proxy(&self) -> Option<ProxyConfig> {
        self.proxy.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    async fn password_login(
        &self,
        username: &str,
        password: &SecretString,
    ) -> Result<Session, BotdError> {
        let front = self
            .testing
            .as_ref()
            .and_then(|t| t.front_base_url.clone().or(t.backpage_base_url.clone()));
        let proxy = self.proxy();
        let mut session = match (&front, &proxy) {
            (Some(url), Some(proxy)) => {
                xenomit_backpage::login::login_with_credentials_at_via(
                    url,
                    username,
                    password.reveal(),
                    proxy,
                )
                .await
            }
            (Some(url), None) => {
                xenomit_backpage::login::login_with_credentials_at(url, username, password.reveal())
                    .await
            }
            (None, Some(proxy)) => {
                xenomit_backpage::login::login_with_credentials_via(
                    username,
                    password.reveal(),
                    proxy,
                )
                .await
            }
            (None, None) => {
                xenomit_backpage::login::login_with_credentials(username, password.reveal()).await
            }
        }
        .map_err(|error| map_auth_error(&error, "Backpage sign-in failed"))?;
        session.set_instance_base(
            self.testing
                .as_ref()
                .and_then(|t| t.backpage_base_url.clone()),
        );
        self.validate(session).await
    }

    async fn sid_login(
        &self,
        dosid: &SecretString,
        server: Option<&str>,
    ) -> Result<Session, BotdError> {
        let server = server
            .filter(|s| !s.trim().is_empty())
            .unwrap_or(&self.account.server);
        let mut session = Session::new(dosid.reveal(), server);
        session.set_instance_base(
            self.testing
                .as_ref()
                .and_then(|t| t.backpage_base_url.clone()),
        );
        self.validate(session).await
    }

    /// Load the Flash contract — proves the dosid and fills flashvars.
    /// With an armed proxy the whole check routes through it; a working
    /// login also records the observed exit IP.
    async fn validate(&self, session: Session) -> Result<Session, BotdError> {
        let proxy = self.proxy();
        let mut client = match &proxy {
            Some(proxy) => xenomit_backpage::BackpageClient::with_proxy(session, proxy),
            None => xenomit_backpage::BackpageClient::new(session),
        }
        .map_err(|error| map_auth_error(&error, "session is unusable"))?;
        xenomit_backpage::launch::load(&mut client)
            .await
            .map_err(|error| map_auth_error(&error, "Backpage rejected the session"))?;
        if proxy.is_some() {
            match client.exit_ip(self.exit_ip_url.as_deref()).await {
                Ok(ip) => {
                    info!(exit_ip = %ip, "proxy exit IP observed");
                    *self.exit_ip.lock().unwrap_or_else(|e| e.into_inner()) = Some(ip);
                }
                // Egress observability — a dead echo never fails the login.
                Err(error) => warn!(%error, "exit-IP probe failed"),
            }
        }
        Ok(client.session().clone())
    }

    /// The configured account server (for status/panel identity).
    pub fn server(&self) -> &str {
        &self.account.server
    }
}

impl SessionProvider for HeadlessSessionProvider {
    fn authenticate(&self) -> Result<Session, String> {
        HeadlessSessionProvider::authenticate(self).map_err(|e| e.to_string())
    }
}

fn map_auth_error(error: &xenomit_backpage::Error, context: &str) -> BotdError {
    use xenomit_backpage::Error;
    let message = format!("{context}: {error}");
    // Egress failure → exit 69 so the operator re-proxies the pod.
    if error.is_proxy_failure() {
        return BotdError::ProxyFailure(message);
    }
    match error {
        Error::CaptchaRequired => BotdError::CaptchaRequired(message),
        Error::WrongCredentials | Error::SessionExpired | Error::InvalidSession => {
            BotdError::CredentialsRejected(message)
        }
        Error::InvalidProxy(_) => BotdError::Config(message),
        Error::Http(_) | Error::Io(_) => BotdError::Unavailable(message),
        _ => BotdError::Fatal(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AuthMode;
    use crate::secrets::{FileSecretResolver, SecretRef};
    use std::path::PathBuf;

    fn provider(auth: AuthMode, path: PathBuf) -> HeadlessSessionProvider {
        HeadlessSessionProvider::new(
            AccountConfig {
                server: "int1".into(),
                auth,
                credentials: SecretRef::File(path),
            },
            None,
            Arc::new(FileSecretResolver),
            NetworkConfig::default(),
            None,
        )
    }

    #[test]
    fn unarmed_provider_fails_authentication() {
        let provider = provider(AuthMode::Session, PathBuf::from("/nonexistent"));
        let error = provider.authenticate().unwrap_err();
        // Missing file + no armed credentials → unavailable (transient).
        assert!(matches!(error, BotdError::Unavailable(_)));
    }

    #[test]
    fn arm_parses_and_caches_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("creds");
        std::fs::write(&file, r#"{"dosid":"x"}"#).unwrap();
        let provider = provider(AuthMode::Session, file);
        provider.arm().unwrap();
        // Cached armed credentials survive a transient resolve failure —
        // the refresh re-login path relies on this.
        std::fs::remove_file(dir.path().join("creds")).unwrap();
        let creds = provider.credentials().unwrap();
        assert!(matches!(creds, AccountCredentials::Session { .. }));
    }

    #[test]
    fn invalid_credentials_fail_permanently() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("creds");
        std::fs::write(&file, "{}").unwrap();
        let provider = provider(AuthMode::Session, file);
        let error = provider.authenticate().unwrap_err();
        assert!(matches!(error, BotdError::Config(_)));
    }
}
