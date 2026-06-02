//! Global server state: a lock-free registry of live sessions.

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use session_sharing_protocol::common::SessionId;
use tokio::sync::Mutex;

use crate::session::Session;

/// Tunables shared by all connection handlers.
#[derive(Clone, Copy, Debug)]
pub struct Config {
    /// How long a session is retained after its sharer disconnects, awaiting a
    /// `/resume`. If no resume arrives within this window, the session is reaped.
    pub sharer_grace: Duration,
    /// Max inbound WebSocket message size (bytes). Caps a single frame/message
    /// from any peer (e.g. a huge scrollback or PtyBytesRead).
    pub max_message_bytes: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            sharer_grace: Duration::from_secs(120),
            max_message_bytes: 16 * 1024 * 1024,
        }
    }
}

/// All live sessions, keyed by id. `DashMap` so independent sessions never
/// contend a single lock — many concurrent sessions scale across cores.
#[derive(Default)]
pub struct ServerState {
    pub sessions: DashMap<SessionId, Arc<Mutex<Session>>>,
    pub config: Config,
}

impl ServerState {
    pub fn with_config(config: Config) -> Self {
        Self {
            sessions: DashMap::new(),
            config,
        }
    }

    pub fn get(&self, id: &SessionId) -> Option<Arc<Mutex<Session>>> {
        self.sessions.get(id).map(|s| s.clone())
    }

    pub fn insert(&self, id: SessionId, session: Arc<Mutex<Session>>) {
        self.sessions.insert(id, session);
    }

    pub fn remove(&self, id: &SessionId) {
        self.sessions.remove(id);
    }
}
