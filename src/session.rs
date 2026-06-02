//! Per-session state and fan-out.
//!
//! A session has exactly one sharer (which may disconnect and resume) and any
//! number of viewers. Terminal output flows sharer → viewers; control requests
//! flow viewer → sharer. We keep an ordered event log so late joiners and
//! reconnecting viewers can catch up.
//!
//! Delivery is lossless and ordered: each connection has an unbounded mpsc
//! channel to its writer task. We never use a lossy broadcast for terminal
//! bytes — dropping a `PtyBytesRead` would corrupt the viewer's screen.

use std::collections::HashMap;

use session_sharing_protocol::common::{
    ActivePrompt, BlockId, InputReplicaId, OrderedTerminalEvent, ParticipantId, ParticipantInfo,
    ParticipantList, PresentViewer, ProfileData, Role, Scrollback, Sharer, WindowSize,
};
use session_sharing_protocol::sharer::{ReconnectToken, SessionSourceType};
use session_sharing_protocol::{sharer, viewer};
use tokio::sync::mpsc;

/// A message queued for delivery to the sharer's websocket writer.
pub type SharerTx = mpsc::UnboundedSender<sharer::DownstreamMessage>;
/// A message queued for delivery to a viewer's websocket writer.
pub type ViewerTx = mpsc::UnboundedSender<viewer::DownstreamMessage>;

pub struct Session {
    /// Retained for logging/debugging; the registry key is the source of truth.
    #[allow(dead_code)]
    pub session_id: session_sharing_protocol::common::SessionId,
    pub reconnect_token: ReconnectToken,
    pub sharer_id: ParticipantId,
    pub sharer_firebase_uid: String,

    /// Sender to the currently-connected sharer's writer task, if any.
    /// `None` while the sharer is disconnected (awaiting `/resume`).
    pub sharer_tx: Option<SharerTx>,

    /// Connected viewers: id → writer channel.
    pub viewers: HashMap<ParticipantId, ViewerTx>,

    /// Ordered terminal event log. Index is NOT event_no; we store events as
    /// received and rely on `event_no` inside each for client-side ordering.
    pub events: Vec<OrderedTerminalEvent>,
    /// Highest event_no we have stored (for acks + catch-up). `None` if empty.
    pub latest_event_no: Option<usize>,

    // Latest "initial state" needed to bootstrap a freshly-joining viewer.
    pub scrollback: Scrollback,
    pub active_prompt: ActivePrompt,
    pub window_size: WindowSize,
    pub init_block_id: BlockId,
    pub input_replica_id: InputReplicaId,
    pub source_type: SessionSourceType,
    pub source_task_id: Option<String>,

    pub participants: ParticipantList,
}

impl Session {
    pub fn new(
        session_id: session_sharing_protocol::common::SessionId,
        sharer_id: ParticipantId,
        init: sharer::InitPayload,
    ) -> Self {
        let sharer_firebase_uid = format!("self-hosted-sharer-{sharer_id}");
        let participants = ParticipantList {
            sharer: Sharer {
                info: ParticipantInfo {
                    id: sharer_id.clone(),
                    profile_data: ProfileData {
                        firebase_uid: sharer_firebase_uid.clone(),
                        display_name: "Sharer".to_owned(),
                        input_replica_id: init.input_replica_id.clone(),
                        ..Default::default()
                    },
                    selection: init.selection.clone(),
                },
            },
            ..Default::default()
        };

        Self {
            session_id,
            reconnect_token: ReconnectToken::new(),
            sharer_id,
            sharer_firebase_uid,
            sharer_tx: None,
            viewers: HashMap::new(),
            events: Vec::new(),
            latest_event_no: None,
            scrollback: init.scrollback,
            active_prompt: init.active_prompt,
            window_size: init.window_size,
            init_block_id: init.init_block_id,
            input_replica_id: init.input_replica_id,
            source_type: init.source_type,
            source_task_id: init.source_task_id,
            participants,
        }
    }

    /// Append a terminal event and fan it out to all connected viewers.
    pub fn record_and_broadcast_event(&mut self, event: OrderedTerminalEvent) {
        self.latest_event_no = Some(event.event_no);
        // Keep a copy for catch-up, forward the original.
        self.events.push(event.clone());
        self.broadcast_viewers(viewer::DownstreamMessage::OrderedTerminalEvent(event));
    }

    /// Send a message to every connected viewer, dropping any whose channel
    /// has closed (writer task gone).
    pub fn broadcast_viewers(&mut self, msg: viewer::DownstreamMessage) {
        self.viewers.retain(|_, tx| tx.send(msg.clone()).is_ok());
    }

    /// Send a message to the sharer if connected.
    pub fn send_sharer(&self, msg: sharer::DownstreamMessage) {
        if let Some(tx) = &self.sharer_tx {
            let _ = tx.send(msg);
        }
    }

    /// Register a present viewer in the participant list with full access.
    pub fn add_present_viewer(&mut self, id: ParticipantId, input_replica_id: InputReplicaId) {
        let firebase_uid = format!("self-hosted-viewer-{id}");
        self.participants.present_viewers.push(PresentViewer {
            info: ParticipantInfo {
                id,
                profile_data: ProfileData {
                    firebase_uid,
                    display_name: "Viewer".to_owned(),
                    input_replica_id,
                    ..Default::default()
                },
                selection: Default::default(),
            },
            // No limits: everyone gets the highest role.
            max_acl: Role::Full,
        });
    }

    /// Remove a viewer from present list (on disconnect).
    pub fn remove_present_viewer(&mut self, id: &ParticipantId) {
        self.participants
            .present_viewers
            .retain(|v| &v.info.id != id);
    }

    /// Events with `event_no` strictly greater than `after` (catch-up on join).
    pub fn events_after(&self, after: Option<usize>) -> Vec<OrderedTerminalEvent> {
        match after {
            None => self.events.clone(),
            Some(n) => self
                .events
                .iter()
                .filter(|e| e.event_no > n)
                .cloned()
                .collect(),
        }
    }
}
