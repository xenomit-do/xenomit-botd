//! Panel wiring (Agent Protocol v1, `docs/08-agent-fleet.md`).
//!
//! The runtime's [`PanelLink`]/agent thread does the WebSocket work; this
//! module owns botd's payload shapes (mirroring the GUI's hello/status/
//! config bodies) and the settings applied through panel commands.
//! Session credentials are never included — the protocol carries no secret
//! material by design.
use std::sync::Arc;

use serde_json::{Map, Value, json};
use xenomit_bot_runtime::panel::{self, PanelLink, PanelSettings};
use xenomit_bot_runtime::statistics;
use xenomit_bot_runtime::{BotSettings, SharedStats};
use xenomit_game_api::ModuleDescriptor;

use crate::config::BotdConfig;
use crate::secrets::SecretString;
use crate::state::Phase;

/// Configure `link` from the `panel` block. Idempotent — a revision bump
/// makes the running agent reconnect with fresh settings, so arm retries
/// and token rotation can re-call it safely. Spawning the agent thread is
/// the caller's once-only step (`panel::spawn_agent` is not idempotent).
/// The token is injected into `PanelSettings` and never logged.
pub fn configure_link(link: &Arc<PanelLink>, config: &BotdConfig, token: &SecretString) {
    let Some(panel) = &config.panel else {
        return;
    };
    // No base URL (pods without panel pairing) → the link stays disabled.
    let Some(url) = panel.base_url.clone() else {
        return;
    };
    link.configure(PanelSettings {
        enabled: true,
        url,
        token: token.reveal().to_owned(),
        instance_id: config.bot.id.clone(),
        server: config.account.server.clone(),
        user_id: String::new(),
        username: config.bot.label.clone().unwrap_or_default(),
    });
}

/// `hello` body (minus `type`/`protocol`) — same shape the GUI advertises
/// plus `machine` filled by the agent. `modules` is every descriptor the
/// panel can configure (objectives + `__behaviors` + `__plugins`).
pub fn hello_body(
    config: &BotdConfig,
    session: Option<&xenomit_backpage::Session>,
    modules: &[ModuleDescriptor],
    settings: &BotSettings,
) -> Value {
    json!({
        "instance": {
            "server": session
                .map(|s| s.server.clone())
                .unwrap_or_else(|| config.account.server.clone()),
            "user_id": session
                .map(|s| s.user_id.to_string())
                .unwrap_or_default(),
            "username": session
                .and_then(username_of)
                .or_else(|| config.bot.label.clone())
                .unwrap_or_default(),
        },
        "client": "packet",
        "backend": "tcp",
        "bot_id": config.bot.id,
        "tenant": config.bot.tenant,
        "modules": modules
            .iter()
            .filter_map(|descriptor| serde_json::to_value(descriptor).ok())
            .collect::<Vec<Value>>(),
        "config": config_body(settings, modules),
    })
}

fn username_of(session: &xenomit_backpage::Session) -> Option<String> {
    let name = session
        .flashvars
        .get("username")
        .or_else(|| session.flashvars.get("userName"))
        .cloned();
    name.filter(|name| !name.is_empty())
}

/// `config.modules` body — `descriptor.key → value` per advertised module;
/// the link dedupes unchanged bodies.
pub fn config_body(settings: &BotSettings, modules: &[ModuleDescriptor]) -> Value {
    let mut out = Map::new();
    for descriptor in modules {
        let nested = match descriptor.id.as_str() {
            "__behaviors" => settings.behaviors.clone(),
            "__plugins" => json!({ "plugins": settings.plugins }),
            _ => settings.module_config_for(&descriptor.id).settings,
        };
        out.insert(
            descriptor.id.clone(),
            Value::Object(panel::module_settings_flat(descriptor, &nested)),
        );
    }
    json!({
        "active_module": settings.active_module,
        "modules": out,
    })
}

/// `status` body — GUI fields plus managed `phase`/`needs_human`
/// (`docs/08-agent-fleet.md` §8.7). Never carries secret material.
#[allow(clippy::too_many_arguments)]
pub fn status_body(
    phase: Phase,
    bot_running: bool,
    activity: &str,
    session: Option<&xenomit_backpage::Session>,
    statistics: Option<&SharedStats>,
    settings: &BotSettings,
    needs_human: Option<&str>,
) -> Value {
    let now = statistics::now_ms();
    let (balances, rates, uptime_s) = statistics
        .and_then(|stats| stats.lock().ok())
        .map(|stats| {
            (
                stats.balances,
                stats.rates(now),
                now.saturating_sub(stats.started) / 1000,
            )
        })
        .unwrap_or(([None; 4], [0.0; 4], 0));
    json!({
        "bot_running": bot_running,
        "module_id": settings.active_module,
        "activity": activity,
        "uptime_s": uptime_s,
        "sid_expires_at": session.and_then(|s| s.expires_at).map(|end| end * 1000),
        "client": "packet",
        "backend": "tcp",
        "phase": phase.panel_label(),
        "needs_human": needs_human,
        "balances": balances.map(|b| b.unwrap_or(0.0)),
        "rates": {
            "cre_h": rates[0],
            "uri_h": rates[1],
            "exp_h": rates[3],
            "hon_h": rates[2],
        },
    })
}

/// Split `modules_for(all_enabled)` output into the hello/config descriptor
/// list, normalizing `__plugins` setting keys to `plugins.*` like the GUI.
pub fn catalog_descriptors(mut modules: Vec<ModuleDescriptor>) -> Vec<ModuleDescriptor> {
    if let Some(descriptor) = modules.iter_mut().find(|d| d.id == "__plugins") {
        for setting in &mut descriptor.settings {
            if !setting.key.starts_with("plugins.") {
                setting.key = format!("plugins.{}", setting.key);
            }
        }
    }
    modules
}

/// Unflatten a panel `update_config` flat `{key: value}` map into a nested
/// settings object (same semantics as the GUI's `apply_remote_config`).
pub fn unflatten(flat: &Value) -> Result<Value, String> {
    let flat = flat.as_object().ok_or("settings must be an object")?;
    let mut nested = json!({});
    for (key, value) in flat {
        xenomit_game_api::set_setting_value(&mut nested, key, value.clone())?;
    }
    Ok(nested)
}

#[cfg(test)]
mod tests {
    use super::*;
    use xenomit_game_api::{ModuleDescriptor, SettingDescriptor, SettingKind};

    fn descriptor(id: &str) -> ModuleDescriptor {
        ModuleDescriptor::new(id, id).setting(SettingDescriptor::new(
            "enabled",
            "Enabled",
            SettingKind::Boolean,
            json!(true),
        ))
    }

    #[test]
    fn status_body_shape() {
        let settings = BotSettings::default();
        let body = status_body(
            Phase::Run,
            true,
            "collecting",
            None,
            None,
            &settings,
            Some("captcha"),
        );
        assert_eq!(body["client"], "packet");
        assert_eq!(body["phase"], "running");
        assert_eq!(body["needs_human"], "captcha");
        assert_eq!(body["bot_running"], true);
    }

    #[test]
    fn hello_omits_session_material() {
        let mut session = xenomit_backpage::Session::new("dosid-value", "int1");
        session.user_id = 42;
        let mut config: BotdConfig = serde_json::from_value(json!({
            "schema": 1,
            "bot": {"id": "bot_1", "tenant": "t"},
            "account": {"server": "int1", "auth": "session",
                        "credentials": {"file": "/x"}},
            "client": {"kind": "packet"},
        }))
        .unwrap();
        config.panel = None;
        let body = hello_body(
            &config,
            Some(&session),
            &[descriptor("m1")],
            &BotSettings::default(),
        );
        let text = body.to_string();
        assert!(!text.contains("dosid-value"));
        assert_eq!(body["instance"]["user_id"], "42");
        assert_eq!(body["instance"]["server"], "int1.darkorbit.com");
    }

    #[test]
    fn unflatten_roundtrip() {
        let nested = unflatten(&json!({"safety.min_health_percent": 30, "x": true})).unwrap();
        assert_eq!(nested["safety"]["min_health_percent"], 30);
        assert_eq!(nested["x"], true);
        assert!(unflatten(&json!([1])).is_err());
    }
}
