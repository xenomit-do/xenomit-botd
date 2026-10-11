//! Headless [`HostActionHandler`]: buffers runtime host actions for the
//! supervisor to execute (re-login on `Refresh`, drain on `Close`, …).
use std::collections::VecDeque;
use std::sync::Mutex;

use xenomit_behavior_engine::HostAction;
use xenomit_bot_runtime::HostActionHandler;

/// Queue the supervisor drains once per tick. Duplicates of identical
/// pending actions collapse, matching `HostActionQueue`'s behavior.
#[derive(Default)]
pub struct HeadlessHostActions {
    queue: Mutex<VecDeque<HostAction>>,
}

impl HeadlessHostActions {
    pub fn drain(&self) -> Vec<HostAction> {
        self.queue
            .lock()
            .map(|mut queue| queue.drain(..).collect())
            .unwrap_or_default()
    }
}

impl HostActionHandler for HeadlessHostActions {
    fn handle(&self, action: HostAction) {
        if let Ok(mut queue) = self.queue.lock()
            && !queue.contains(&action)
        {
            queue.push_back(action);
        }
    }
}
