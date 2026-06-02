//! Sharer websocket: `/sessions/create` and `/sessions/{id}/resume`.

use std::sync::Arc;

use axum::{
    extract::{
        Path, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::Response,
};
use futures_util::{SinkExt, StreamExt};
use session_sharing_protocol::common::{ParticipantId, SessionId, SessionSecret};
use session_sharing_protocol::sharer::{
    DownstreamMessage, ReconnectionFailedReason, UpstreamMessage,
};
use tokio::sync::{Mutex, mpsc};

use crate::session::{Session, WRITER_CHANNEL_CAP};
use crate::state::ServerState;

pub async fn create(State(state): State<Arc<ServerState>>, ws: WebSocketUpgrade) -> Response {
    let ws = ws.max_message_size(state.config.max_message_bytes);
    ws.on_upgrade(move |socket| handle_create(state, socket))
}

pub async fn resume(
    State(state): State<Arc<ServerState>>,
    Path(session_id): Path<SessionId>,
    ws: WebSocketUpgrade,
) -> Response {
    let ws = ws.max_message_size(state.config.max_message_bytes);
    ws.on_upgrade(move |socket| handle_resume(state, session_id, socket))
}

/// Split the socket into a writer task fed by a *bounded* mpsc, plus the reader
/// half. Returns the sender used to enqueue downstream messages to this sharer.
/// A bounded queue means a stalled sharer socket can't grow memory without limit.
fn spawn_writer(
    mut sink: futures_util::stream::SplitSink<WebSocket, Message>,
) -> mpsc::Sender<DownstreamMessage> {
    let (tx, mut rx) = mpsc::channel::<DownstreamMessage>(WRITER_CHANNEL_CAP);
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match msg.to_json() {
                Ok(json) => {
                    if sink.send(Message::Text(json)).await.is_err() {
                        break;
                    }
                }
                Err(e) => tracing::warn!("sharer serialize failed: {e}"),
            }
        }
        let _ = sink.close().await;
    });
    tx
}

async fn handle_create(state: Arc<ServerState>, socket: WebSocket) {
    let (sink, mut stream) = socket.split();

    // First message must be Initialize.
    let init = loop {
        match stream.next().await {
            Some(Ok(Message::Text(t))) => match UpstreamMessage::from_json(&t) {
                Ok(UpstreamMessage::Initialize(p)) => break p,
                Ok(UpstreamMessage::Ping { data }) => {
                    // Tolerate a ping before init; ignore.
                    let _ = data;
                    continue;
                }
                Ok(other) => {
                    tracing::warn!("sharer sent {:?} before Initialize; ignoring", std::mem::discriminant(&other));
                    continue;
                }
                Err(e) => {
                    tracing::warn!("sharer init parse error: {e}");
                    return;
                }
            },
            Some(Ok(Message::Close(_))) | None => return,
            Some(Ok(_)) => continue,
            Some(Err(e)) => {
                tracing::warn!("sharer ws error before init: {e}");
                return;
            }
        }
    };

    let session_id = SessionId::new();
    let sharer_id = ParticipantId::new();
    let mut session = Session::new(session_id, sharer_id.clone(), init);

    let sharer_tx = spawn_writer(sink);
    session.sharer_tx = Some(sharer_tx.clone());
    session.sharer_epoch += 1;
    let reconnect_token = session.reconnect_token.clone();
    let sharer_firebase_uid = session.sharer_firebase_uid.clone();

    // Acknowledge creation. SessionSecret is unused by our (no-auth) server but
    // the client stores it; send a fresh one.
    let _ = sharer_tx
        .send(DownstreamMessage::SessionInitialized {
            session_id,
            session_secret: SessionSecret::new(),
            reconnect_token,
            sharer_id: sharer_id.clone(),
            sharer_firebase_uid,
        })
        .await;

    let session = Arc::new(Mutex::new(session));
    state.insert(session_id, session.clone());
    tracing::info!(%session_id, "session created");

    run_sharer_loop(state, session, session_id, sharer_id, stream).await;
}

async fn handle_resume(
    state: Arc<ServerState>,
    session_id: SessionId,
    socket: WebSocket,
) {
    let (sink, mut stream) = socket.split();

    // First message must be Reconnect.
    let reconnect = loop {
        match stream.next().await {
            Some(Ok(Message::Text(t))) => match UpstreamMessage::from_json(&t) {
                Ok(UpstreamMessage::Reconnect(p)) => break p,
                Ok(_) => continue,
                Err(e) => {
                    tracing::warn!("sharer resume parse error: {e}");
                    return;
                }
            },
            Some(Ok(Message::Close(_))) | None => return,
            Some(Ok(_)) => continue,
            Some(Err(_)) => return,
        }
    };

    let Some(session) = state.get(&session_id) else {
        // Session is gone; tell the sharer and close.
        let tx = spawn_writer(sink);
        let _ = tx
            .send(DownstreamMessage::FailedToReconnect {
                reason: ReconnectionFailedReason::SessionNotFound,
            })
            .await;
        return;
    };

    let sharer_tx = spawn_writer(sink);
    let (sharer_id, last_event_no, participant_list) = {
        let mut s = session.lock().await;

        // SECURITY (#1): only the holder of the reconnect token issued at
        // creation may take over the sharer role. Without this check, anyone
        // who knows the session_id could hijack the sharer side and feed
        // forged terminal output to viewers / intercept their control requests.
        if reconnect.reconnect_token != s.reconnect_token {
            tracing::warn!(%session_id, "resume rejected: wrong reconnect token");
            drop(s);
            let _ = sharer_tx
                .send(DownstreamMessage::FailedToReconnect {
                    reason: ReconnectionFailedReason::WrongReconnectionToken,
                })
                .await;
            return;
        }

        s.sharer_tx = Some(sharer_tx.clone());
        s.sharer_epoch += 1;
        (
            s.sharer_id.clone(),
            s.latest_event_no,
            s.participants.clone(),
        )
    };

    let _ = sharer_tx
        .send(DownstreamMessage::SessionReconnected {
            last_received_event_no: last_event_no,
            participant_list,
        })
        .await;
    tracing::info!(%session_id, "sharer resumed");

    run_sharer_loop(state, session, session_id, sharer_id, stream).await;
}

async fn run_sharer_loop(
    state: Arc<ServerState>,
    session: Arc<Mutex<Session>>,
    session_id: SessionId,
    sharer_id: ParticipantId,
    mut stream: futures_util::stream::SplitStream<WebSocket>,
) {
    while let Some(frame) = stream.next().await {
        let text = match frame {
            Ok(Message::Text(t)) => t,
            Ok(Message::Binary(_)) | Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => continue,
            Ok(Message::Close(_)) | Err(_) => break,
        };
        let msg = match UpstreamMessage::from_json(&text) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!("sharer msg parse error: {e}");
                continue;
            }
        };

        let mut s = session.lock().await;
        match msg {
            UpstreamMessage::Initialize(_) | UpstreamMessage::Reconnect(_) => {
                // Already handled in the connect/resume handshake.
            }
            UpstreamMessage::OrderedTerminalEvent(event) => {
                let ack_no = event.event_no;
                s.record_and_broadcast_event(event);
                // Immediately ack so the sharer can free its unacked buffer.
                s.send_sharer(DownstreamMessage::EventsProcessedAck {
                    latest_processed_event_no: ack_no,
                });
            }
            UpstreamMessage::UpdateActivePrompt(update) => {
                s.active_prompt = update.active_prompt.clone();
                s.broadcast_viewers(
                    session_sharing_protocol::viewer::DownstreamMessage::ActivePromptUpdated(update),
                );
            }
            UpstreamMessage::UpdateUniversalDeveloperInputContext(update) => {
                s.broadcast_viewers(
                    session_sharing_protocol::viewer::DownstreamMessage::UniversalDeveloperInputContextUpdated(update),
                );
            }
            UpstreamMessage::UpdateSelection(update) => {
                // Update the sharer's stored selection for presence and notify viewers.
                s.participants.sharer.info.selection = update.selection.clone();
                let presence = presence_update_for(&s.sharer_id.clone(), update.selection);
                s.broadcast_viewers(
                    session_sharing_protocol::viewer::DownstreamMessage::ParticipantPresenceUpdated(
                        presence,
                    ),
                );
            }
            UpstreamMessage::UpdateInput(update) => {
                s.broadcast_viewers(
                    session_sharing_protocol::viewer::DownstreamMessage::InputUpdated(update),
                );
            }
            UpstreamMessage::Ping { data } => {
                s.send_sharer(DownstreamMessage::Pong { data });
            }
            UpstreamMessage::EndSession { reason: _ } => {
                let ended = session_sharing_protocol::viewer::SessionEndedReason::EndedBySharer;
                s.broadcast_viewers(
                    session_sharing_protocol::viewer::DownstreamMessage::SessionEnded {
                        reason: ended,
                    },
                );
                drop(s);
                state.remove(&session_id);
                tracing::info!(%session_id, "session ended by sharer");
                return;
            }
            // Role / ACL / guest management: no limits, so these are best-effort
            // broadcasts or no-ops. We forward role changes to viewers so the UI
            // stays consistent, and ignore guest/ACL bookkeeping.
            UpstreamMessage::UpdateRole { participant_id, role } => {
                s.broadcast_viewers(
                    session_sharing_protocol::viewer::DownstreamMessage::ParticipantRoleChanged {
                        participant_id,
                        reason: Default::default(),
                        role,
                    },
                );
            }
            UpstreamMessage::RespondToRoleRequest { .. }
            | UpstreamMessage::UpdateUserRole { .. }
            | UpstreamMessage::UpdatePendingUserRole { .. }
            | UpstreamMessage::UpdateAllRolesToReader { .. }
            | UpstreamMessage::RejectInputUpdate { .. }
            | UpstreamMessage::RejectCommandExecutionRequest { .. }
            | UpstreamMessage::RejectWriteToPtyRequest { .. }
            | UpstreamMessage::RejectAgentPromptRequest { .. }
            | UpstreamMessage::RejectControlActionRequest { .. }
            | UpstreamMessage::UpdateLinkAccessLevel { .. }
            | UpstreamMessage::UpdateTeamAccessLevel { .. }
            | UpstreamMessage::AddGuests { .. }
            | UpstreamMessage::RemoveGuest { .. }
            | UpstreamMessage::RemovePendingGuest { .. }
            | UpstreamMessage::ExtendSessionRetention { .. } => {
                // Intentionally unhandled: no quotas, no ACL enforcement.
            }
        }
    }

    // Sharer disconnected (socket dropped without EndSession). Mark the sharer
    // gone and snapshot the epoch, then spawn a reaper: after the grace period,
    // if no `/resume` has bumped the epoch, the session is dead — notify any
    // viewers and remove it. This prevents permanently-leaked sessions (#3).
    let reap_epoch = {
        let mut s = session.lock().await;
        s.sharer_tx = None;
        s.sharer_epoch
    };
    let _ = sharer_id;
    tracing::info!(%session_id, "sharer disconnected (grace period started)");

    let grace = state.config.sharer_grace;
    tokio::spawn(async move {
        tokio::time::sleep(grace).await;
        let mut s = session.lock().await;
        // A resume (or a brand-new create reusing the Arc — impossible here)
        // would have bumped the epoch. Unchanged => still orphaned.
        if s.sharer_epoch != reap_epoch || s.sharer_tx.is_some() {
            return;
        }
        s.broadcast_viewers(session_sharing_protocol::viewer::DownstreamMessage::SessionEnded {
            reason: session_sharing_protocol::viewer::SessionEndedReason::EndedBySharer,
        });
        drop(s);
        state.remove(&session_id);
        tracing::info!(%session_id, "session reaped after sharer grace period");
    });
}

fn presence_update_for(
    id: &ParticipantId,
    selection: session_sharing_protocol::common::Selection,
) -> session_sharing_protocol::common::ParticipantPresenceUpdate {
    session_sharing_protocol::common::ParticipantPresenceUpdate {
        participant_id: id.clone(),
        update: session_sharing_protocol::common::PresenceUpdate::Selection(selection),
    }
}
