//! Egress proxy definition + connectivity check (`docs/05-network.md`).
//!
//! At Arm `botd` verifies the proxy is reachable; Backpage login and the
//! packet-game attach then route through it via the clients' `*_via`
//! APIs. There is no direct fallback — a proxy failure maps to
//! [`crate::error::BotdError::ProxyFailure`] (exit 69).
use std::fmt;
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use crate::secrets::{SecretError, SecretString};

/// Supported egress schemes.
const SCHEMES: [&str; 4] = ["socks5", "socks5h", "http", "https"];

#[derive(Clone)]
pub struct ProxySpec {
    pub scheme: String,
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<SecretString>,
}

impl ProxySpec {
    /// `scheme://host:port` — the URL both client proxy types consume.
    pub fn url(&self) -> String {
        format!("{}://{}:{}", self.scheme, self.host, self.port)
    }

    /// Backpage (HTTP) proxy config — reqwest `CONNECT`/absolute-form.
    pub fn to_backpage_config(&self) -> xenomit_backpage::ProxyConfig {
        match &self.username {
            Some(user) => xenomit_backpage::ProxyConfig::with_auth(
                self.url(),
                user.clone(),
                self.password
                    .as_ref()
                    .map(|p| p.reveal().to_owned())
                    .unwrap_or_default(),
            ),
            None => xenomit_backpage::ProxyConfig::new(self.url()),
        }
    }

    /// Packet-client (game TCP) proxy config — CONNECT or SOCKS5 dial.
    pub fn to_packet_config(&self) -> xenomit_packet_client::proxy::ProxyConfig {
        match &self.username {
            Some(user) => xenomit_packet_client::proxy::ProxyConfig::with_auth(
                self.url(),
                user.clone(),
                self.password
                    .as_ref()
                    .map(|p| p.reveal().to_owned())
                    .unwrap_or_default(),
            ),
            None => xenomit_packet_client::proxy::ProxyConfig::new(self.url()),
        }
    }
}

impl fmt::Debug for ProxySpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProxySpec")
            .field("scheme", &self.scheme)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

fn invalid(message: impl Into<String>) -> SecretError {
    SecretError::Invalid(format!("network.proxy: {}", message.into()))
}

/// Parse `scheme://[user[:pass]@]host:port` — no URL crate needed.
fn parse_url(url: &str) -> Result<ProxySpec, SecretError> {
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| invalid("expected scheme://host:port"))?;
    let scheme = scheme.to_ascii_lowercase();
    if !SCHEMES.contains(&scheme.as_str()) {
        return Err(invalid(format!(
            "unsupported scheme {scheme:?} (expected one of {SCHEMES:?})"
        )));
    }
    let authority = rest.split('/').next().unwrap_or_default();
    let (userinfo, hostport) = match authority.rsplit_once('@') {
        Some((userinfo, hostport)) => (Some(userinfo), hostport),
        None => (None, authority),
    };
    let (username, password) = match userinfo {
        Some(userinfo) => {
            let (user, pass) = userinfo.split_once(':').unwrap_or((userinfo, ""));
            if user.is_empty() {
                return Err(invalid("proxy username is empty"));
            }
            (
                Some(user.to_owned()),
                (!pass.is_empty()).then(|| SecretString::new(pass)),
            )
        }
        None => (None, None),
    };
    let (host, port) = hostport
        .rsplit_once(':')
        .ok_or_else(|| invalid("expected host:port"))?;
    if host.is_empty() {
        return Err(invalid("proxy host is empty"));
    }
    let port: u16 = port
        .parse()
        .map_err(|_| invalid(format!("invalid port {port:?}")))?;
    Ok(ProxySpec {
        scheme,
        host: host.to_owned(),
        port,
        username,
        password,
    })
}

/// Interpret the `network.proxy` secret payload: `{"scheme","host","port",
/// "username","password"}` or `{"url": "socks5://user:pass@host:1080"}` or a
/// bare URL string.
pub fn parse_proxy(raw: &SecretString) -> Result<ProxySpec, SecretError> {
    let text = raw.reveal().trim();
    if text.is_empty() {
        return Err(invalid("secret is empty"));
    }
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(text)
        && let Some(object) = value.as_object()
    {
        if let Some(url) = object.get("url").and_then(|v| v.as_str()) {
            return parse_url(url);
        }
        let get = |key: &str| {
            object
                .get(key)
                .and_then(|v| v.as_str())
                .map(str::to_owned)
                .filter(|s| !s.is_empty())
        };
        let scheme = get("scheme")
            .unwrap_or_else(|| "socks5".into())
            .to_ascii_lowercase();
        if !SCHEMES.contains(&scheme.as_str()) {
            return Err(invalid(format!("unsupported scheme {scheme:?}")));
        }
        // The API's fixed-proxy credential shape is `{"addr": "host:port",
        // …}`; pool secrets store `{"host": …, "port": …}`.
        let (host, port) = match (get("host"), get("addr")) {
            (Some(host), _) => {
                let port = object
                    .get("port")
                    .and_then(|v| v.as_u64().or_else(|| v.as_str()?.parse().ok()))
                    .ok_or_else(|| invalid("missing or invalid port"))?;
                (host, port)
            }
            (None, Some(addr)) => {
                let (host, port) = addr
                    .rsplit_once(':')
                    .ok_or_else(|| invalid("addr must be host:port"))?;
                if host.is_empty() {
                    return Err(invalid("addr host is empty"));
                }
                (
                    host.to_owned(),
                    port.parse().map_err(|_| invalid("addr port invalid"))?,
                )
            }
            _ => return Err(invalid("missing host")),
        };
        let port = u16::try_from(port).map_err(|_| invalid("port out of range"))?;
        return Ok(ProxySpec {
            scheme,
            host,
            port,
            username: get("username").or_else(|| get("user")),
            password: get("password")
                .or_else(|| get("pass"))
                .map(SecretString::new),
        });
    }
    parse_url(text)
}

/// TCP reachability check — proves the host:port answers without sending
/// credentials or traffic through it.
pub fn check_egress(spec: &ProxySpec, timeout: Duration) -> Result<(), String> {
    let target = format!("{}:{}", spec.host, spec.port);
    let addresses = target
        .to_socket_addrs()
        .map_err(|e| format!("Could not resolve proxy {target}: {e}"))?;
    let mut last = String::new();
    for address in addresses {
        match TcpStream::connect_timeout(&address, timeout) {
            Ok(_) => return Ok(()),
            Err(e) => last = e.to_string(),
        }
    }
    Err(format!("Could not connect to proxy {target}: {last}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(raw: &str) -> Result<ProxySpec, SecretError> {
        parse_proxy(&SecretString::new(raw))
    }

    #[test]
    fn parses_url_forms() {
        let p = spec("socks5://user:pass@proxy.local:1080").unwrap();
        assert_eq!(
            (p.scheme.as_str(), p.host.as_str(), p.port),
            ("socks5", "proxy.local", 1080)
        );
        assert_eq!(p.username.as_deref(), Some("user"));
        assert_eq!(p.password.unwrap().reveal(), "pass");
        let p = spec("http://proxy.local:3128").unwrap();
        assert_eq!(p.username, None);
        assert!(spec("ftp://x:1").is_err());
        assert!(spec("socks5://noport").is_err());
    }

    #[test]
    fn parses_json_forms() {
        let p =
            spec(r#"{"scheme":"socks5h","host":"p","port":1080,"username":"u","password":"w"}"#)
                .unwrap();
        assert_eq!(p.scheme, "socks5h");
        assert_eq!(p.password.unwrap().reveal(), "w");
        let p = spec(r#"{"url":"socks5://u:p@h:9"}"#).unwrap();
        assert_eq!(p.port, 9);
        assert!(spec(r#"{"host":"p"}"#).is_err());
    }

    #[test]
    fn debug_redacts_password() {
        let p = spec(r#"{"scheme":"socks5","host":"h","port":1,"password":"w"}"#).unwrap();
        let dbg = format!("{p:?}");
        assert!(!dbg.contains("\"w\""));
        assert!(dbg.contains("redacted"));
    }
}
