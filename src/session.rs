//! Per-session state and fan-out.
//!
//! A session has exactly one sharer (which may disconnect and resume) and any
//! number of viewers. Terminal output flows sharer → viewers; control requests
//! flow viewer → sharer. We keep an ordered event log so late joiners and
//! reconnecting viewers can catch up.
//!
//! Delivery is ordered, and lossless up to a bound: each connection has a
//! *bounded* mpsc channel to its writer task. We never use a lossy broadcast for
//! terminal bytes — dropping a `PtyBytesRead` mid-stream would corrupt the
//! viewer's screen. Instead, if a peer's queue fills (a stalled/slow socket),
//! we drop that peer entirely; it can reconnect and catch up from the event log.
//! This bounds per-connection memory regardless of peer behavior.

use std::collections::{HashMap, VecDeque};

use session_sharing_protocol::common::{
    ActivePrompt, BlockId, InputReplicaId, OrderedTerminalEvent, ParticipantId, ParticipantInfo,
    ParticipantList, PresentViewer, ProfileData, Role, Scrollback, Sharer, WindowSize,
};
use session_sharing_protocol::sharer::{ReconnectToken, SessionSourceType};
use session_sharing_protocol::{sharer, viewer};
use tokio::sync::mpsc;

/// A message queued for delivery to the sharer's websocket writer.
pub type SharerTx = mpsc::Sender<sharer::DownstreamMessage>;
/// A message queued for delivery to a viewer's websocket writer.
pub type ViewerTx = mpsc::Sender<viewer::DownstreamMessage>;

/// Per-connection writer queue depth. A peer whose socket stalls long enough to
/// fill this is dropped (and may reconnect). Bounds per-connection memory.
pub const WRITER_CHANNEL_CAP: usize = 2048;

/// Max terminal events retained for catch-up. Oldest are dropped past this, so
/// a session's event log uses O(MAX_EVENT_LOG) memory no matter how long it
/// runs. A reconnecting viewer that is further behind than this window will
/// miss the trimmed events (acceptable for a relay; the screen self-heals on
/// subsequent output).
pub const MAX_EVENT_LOG: usize = 16_384;

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

    /// Bumped every time a sharer connects or resumes. The disconnect reaper
    /// captures the epoch at disconnect time and only reaps if it is unchanged
    /// after the grace period — so a successful `/resume` cancels the reap
    /// without any shared timer state.
    pub sharer_epoch: u64,

    /// Connected viewers: id → writer channel.
    pub viewers: HashMap<ParticipantId, ViewerTx>,

    /// Ordered terminal event log (ring buffer, capped at `MAX_EVENT_LOG`).
    /// Index is NOT event_no; we store events as received and rely on `event_no`
    /// inside each for client-side ordering and catch-up.
    pub events: VecDeque<OrderedTerminalEvent>,
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
            sharer_epoch: 0,
            viewers: HashMap::new(),
            events: VecDeque::new(),
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
    /// The log is a ring buffer capped at `MAX_EVENT_LOG`.
    pub fn record_and_broadcast_event(&mut self, event: OrderedTerminalEvent) {
        self.latest_event_no = Some(event.event_no);
        self.events.push_back(event.clone());
        while self.events.len() > MAX_EVENT_LOG {
            self.events.pop_front();
        }
        self.broadcast_viewers(viewer::DownstreamMessage::OrderedTerminalEvent(event));
    }

    /// Send a message to every connected viewer. A viewer whose writer queue is
    /// closed (task gone) or full (socket stalled) is dropped — we never block
    /// the session lock on a slow peer. Dropped viewers can reconnect and catch
    /// up from the event log.
    pub fn broadcast_viewers(&mut self, msg: viewer::DownstreamMessage) {
        use mpsc::error::TrySendError;
        self.viewers.retain(|id, tx| match tx.try_send(msg.clone()) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) => {
                tracing::warn!(viewer = %id, "viewer writer queue full; dropping slow viewer");
                false
            }
            Err(TrySendError::Closed(_)) => false,
        });
    }

    /// Send a message to the sharer if connected. Non-blocking; a full/closed
    /// sharer queue is dropped silently (the sharer loop will observe the
    /// disconnect and the reaper will handle cleanup).
    pub fn send_sharer(&self, msg: sharer::DownstreamMessage) {
        if let Some(tx) = &self.sharer_tx {
            let _ = tx.try_send(msg);
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
            None => self.events.iter().cloned().collect(),
            Some(n) => self
                .events
                .iter()
                .filter(|e| e.event_no > n)
                .cloned()
                .collect(),
        }
    }
}
