//! Global server state: a lock-free registry of live sessions.

use std::sync::Arc;

use dashmap::DashMap;
use session_sharing_protocol::common::SessionId;
use tokio::sync::Mutex;

use crate::session::Session;

/// All live sessions, keyed by id. `DashMap` so independent sessions never
/// contend a single lock — many concurrent sessions scale across cores.
#[derive(Default)]
pub struct ServerState {
    pub sessions: DashMap<SessionId, Arc<Mutex<Session>>>,
}

impl ServerState {
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
