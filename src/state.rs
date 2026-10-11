//! Startup state machine phases (`docs/02-headless-bot.md` §2.3).
use std::fmt;

/// `Boot → LoadConfig → Arm → Activate → Login → Attach → Run`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Phase {
    Boot,
    LoadConfig,
    /// Resolving secrets, wiring resolvers, checking egress.
    Arm,
    /// Waiting for the control plane's activation grant (standalone = instant).
    AwaitActivate,
    /// Backpage authentication.
    Login,
    /// Game-client attach (packet handshake, statistics, panel hello).
    Attach,
    /// Attached and (optionally) running the configured module.
    Run,
    /// Draining after a shutdown request.
    Draining,
    /// Worker drained; writing the epoch-conditional snapshot (docs/03 §3.2).
    Snapshotting,
    /// Snapshot committed; logging out of the game session.
    LoggingOut,
}

impl Phase {
    /// `status.phase` vocabulary from `docs/08-agent-fleet.md` §8.7.
    pub fn panel_label(self) -> &'static str {
        match self {
            Self::Boot | Self::LoadConfig | Self::Arm => "boot",
            Self::AwaitActivate => "pending",
            Self::Login | Self::Attach => "starting",
            Self::Run => "running",
            Self::Draining | Self::Snapshotting | Self::LoggingOut => "draining",
        }
    }
}

impl fmt::Display for Phase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Boot => "boot",
            Self::LoadConfig => "load_config",
            Self::Arm => "arm",
            Self::AwaitActivate => "await_activate",
            Self::Login => "login",
            Self::Attach => "attach",
            Self::Run => "run",
            Self::Draining => "draining",
            Self::Snapshotting => "snapshotting",
            Self::LoggingOut => "logging_out",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_orders_monotonically() {
        let order = [
            Phase::Boot,
            Phase::LoadConfig,
            Phase::Arm,
            Phase::AwaitActivate,
            Phase::Login,
            Phase::Attach,
            Phase::Run,
        ];
        for pair in order.windows(2) {
            assert!(
                pair[0] < pair[1],
                "{:?} must precede {:?}",
                pair[0],
                pair[1]
            );
        }
        assert!(Phase::Draining > Phase::Boot);
    }

    #[test]
    fn panel_labels_match_fleet_vocabulary() {
        assert_eq!(Phase::Boot.panel_label(), "boot");
        assert_eq!(Phase::LoadConfig.panel_label(), "boot");
        assert_eq!(Phase::Arm.panel_label(), "boot");
        assert_eq!(Phase::AwaitActivate.panel_label(), "pending");
        assert_eq!(Phase::Login.panel_label(), "starting");
        assert_eq!(Phase::Attach.panel_label(), "starting");
        assert_eq!(Phase::Run.panel_label(), "running");
        assert_eq!(Phase::Draining.panel_label(), "draining");
    }
}
