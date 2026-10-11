//! Deterministic error → exit-code mapping (`docs/02-headless-bot.md` §2.6).
use std::fmt;

/// Process exit codes. `64`+ is `sysexits.h`-aligned so the orchestrator can
/// distinguish operator action items (credentials, CAPTCHA, fencing, version)
/// from crashes that justify an automatic restart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum ExitCode {
    /// Clean drain: operator/panel/SIGTERM shutdown or completed session.
    Clean = 0,
    /// Crash or non-specific failure — restartable by the supervisor.
    Crash = 1,
    /// Invalid config, unresolvable secrets, unusable deployment.
    InvalidConfig = 64,
    /// Backpage credentials or session rejected — human re-provision needed.
    CredentialsRejected = 65,
    /// Login demands a CAPTCHA — human or captcha service needed.
    CaptchaRequired = 66,
    /// Another session holds the account; this instance lost the fence.
    Fenced = 67,
    /// Game version hash rejected — bot/client update required.
    VersionMismatch = 68,
    /// Proxy required but unreachable or failed.
    ProxyFailure = 69,
    /// Strict-mode handoff snapshot unreadable.
    SnapshotUnreadable = 70,
}

impl ExitCode {
    pub fn code(self) -> i32 {
        self as i32
    }
}

/// Failure taxonomy the supervisor maps to [`ExitCode`].
#[derive(Debug)]
pub enum BotdError {
    /// Config/secret/deployment problem → 64.
    Config(String),
    /// Credential or session rejection → 65.
    CredentialsRejected(String),
    /// Backpage CAPTCHA wall → 66.
    CaptchaRequired(String),
    /// Session fencing lost → 67.
    Fenced(String),
    /// Game version hash rejected → 68.
    VersionMismatch(String),
    /// Egress proxy failure → 69.
    ProxyFailure(String),
    /// Unreadable strict snapshot → 70.
    SnapshotUnreadable(String),
    /// Transient failure within a phase — retried, then escalated to Fatal.
    Unavailable(String),
    /// Anything else → 1.
    Fatal(String),
    /// Valid config for a phase-≥2 feature (control plane, Vault) → 64.
    NotImplemented(String),
}

impl BotdError {
    pub fn exit_code(&self) -> ExitCode {
        match self {
            Self::Config(_) | Self::NotImplemented(_) => ExitCode::InvalidConfig,
            Self::CredentialsRejected(_) => ExitCode::CredentialsRejected,
            Self::CaptchaRequired(_) => ExitCode::CaptchaRequired,
            Self::Fenced(_) => ExitCode::Fenced,
            Self::VersionMismatch(_) => ExitCode::VersionMismatch,
            Self::ProxyFailure(_) => ExitCode::ProxyFailure,
            Self::SnapshotUnreadable(_) => ExitCode::SnapshotUnreadable,
            Self::Unavailable(_) | Self::Fatal(_) => ExitCode::Crash,
        }
    }

    /// Whether the operator must act — surfaced as `needs_human` in panel
    /// status and kept in `last_error` for `/readyz` detail.
    pub fn needs_human(&self) -> Option<&'static str> {
        match self {
            Self::CredentialsRejected(_) => Some("credentials"),
            Self::CaptchaRequired(_) => Some("captcha"),
            Self::Fenced(_) => Some("fenced"),
            Self::VersionMismatch(_) => Some("version"),
            _ => None,
        }
    }
}

impl fmt::Display for BotdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (kind, message) = match self {
            Self::Config(m) => ("config", m),
            Self::CredentialsRejected(m) => ("credentials rejected", m),
            Self::CaptchaRequired(m) => ("captcha required", m),
            Self::Fenced(m) => ("fenced", m),
            Self::VersionMismatch(m) => ("version mismatch", m),
            Self::ProxyFailure(m) => ("proxy failure", m),
            Self::SnapshotUnreadable(m) => ("snapshot unreadable", m),
            Self::Unavailable(m) => ("unavailable", m),
            Self::Fatal(m) => ("fatal", m),
            Self::NotImplemented(m) => ("not implemented", m),
        };
        write!(f, "{kind}: {message}")
    }
}

impl std::error::Error for BotdError {}

/// Packet-client attach failures arrive as strings from
/// `packet_client::connect_with`; classify the known wire-level causes.
pub fn classify_attach_error(message: &str) -> BotdError {
    if message.starts_with("proxy:") {
        BotdError::ProxyFailure(format!("game attach via proxy failed: {message}"))
    } else if message.contains("version hash rejected") {
        BotdError::VersionMismatch(message.into())
    } else if message.contains("login rejected") {
        BotdError::CredentialsRejected(format!("game login rejected: {message}"))
    } else {
        BotdError::Fatal(message.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mapping_is_deterministic() {
        let cases = [
            (BotdError::Config("x".into()), ExitCode::InvalidConfig),
            (
                BotdError::NotImplemented("x".into()),
                ExitCode::InvalidConfig,
            ),
            (
                BotdError::CredentialsRejected("x".into()),
                ExitCode::CredentialsRejected,
            ),
            (
                BotdError::CaptchaRequired("x".into()),
                ExitCode::CaptchaRequired,
            ),
            (BotdError::Fenced("x".into()), ExitCode::Fenced),
            (
                BotdError::VersionMismatch("x".into()),
                ExitCode::VersionMismatch,
            ),
            (BotdError::ProxyFailure("x".into()), ExitCode::ProxyFailure),
            (
                BotdError::SnapshotUnreadable("x".into()),
                ExitCode::SnapshotUnreadable,
            ),
            (BotdError::Unavailable("x".into()), ExitCode::Crash),
            (BotdError::Fatal("x".into()), ExitCode::Crash),
        ];
        for (error, expected) in cases {
            assert_eq!(error.exit_code(), expected, "{error}");
        }
        assert_eq!(ExitCode::Clean.code(), 0);
        assert_eq!(ExitCode::SnapshotUnreadable.code(), 70);
    }

    #[test]
    fn needs_human_kinds() {
        assert_eq!(
            BotdError::CaptchaRequired(String::new()).needs_human(),
            Some("captcha")
        );
        assert_eq!(
            BotdError::Fenced(String::new()).needs_human(),
            Some("fenced")
        );
        assert_eq!(BotdError::Fatal(String::new()).needs_human(), None);
    }

    #[test]
    fn attach_error_classification() {
        assert!(matches!(
            classify_attach_error("Game login failed: version hash rejected by server"),
            BotdError::VersionMismatch(_)
        ));
        assert!(matches!(
            classify_attach_error("Game login failed: login rejected with status 3"),
            BotdError::CredentialsRejected(_)
        ));
        assert!(matches!(
            classify_attach_error("Could not connect to the map server"),
            BotdError::Fatal(_)
        ));
    }
}
