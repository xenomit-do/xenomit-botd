//! Read-only [`SettingsStore`] serving the config's `settings` block.
use xenomit_bot_runtime::{BotSettings, SettingsStore};

/// `botd` settings come from `botd.json` and are immutable at runtime —
/// the control plane owns edits in later phases, so `save` refuses.
pub struct ConfigSettingsStore {
    settings: BotSettings,
}

impl ConfigSettingsStore {
    pub fn new(settings: BotSettings) -> Self {
        Self { settings }
    }
}

impl SettingsStore for ConfigSettingsStore {
    fn load(&self) -> BotSettings {
        self.settings.clone()
    }

    fn save(&self, _settings: &BotSettings) -> Result<(), String> {
        Err("botd settings are read-only — edit the config file".into())
    }
}
