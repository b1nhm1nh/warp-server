//! Viewer websocket: `/sessions/join/{id}` (also used for reconnect).

use std::sync::Arc;

use axum::{
    extract::{
        Path, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::Response,
};
use futures_util::{SinkExt, StreamExt};
use session_sharing_protocol::common::{
    CommandExecutionRequestId, ParticipantId, PresenceUpdate, SessionId,
};
use session_sharing_protocol::sharer;
use session_sharing_protocol::viewer::{DownstreamMessage, FailedToJoinReason, UpstreamMessage};
use tokio::sync::{Mutex, mpsc};

use crate::session::Session;
use crate::state::ServerState;

pub async fn join(
    State(state): State<Arc<ServerState>>,
    Path(session_id): Path<SessionId>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.on_upgrade(move |socket| handle_join(state, session_id, socket))
}

fn spawn_writer(
    mut sink: futures_util::stream::SplitSink<WebSocket, Message>,
) -> mpsc::UnboundedSender<DownstreamMessage> {
    let (tx, mut rx) = mpsc::unbounded_channel::<DownstreamMessage>();
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match msg.to_json() {
                Ok(json) => {
                    if sink.send(Message::Text(json)).await.is_err() {
                        break;
                    }
                }
                Err(e) => tracing::warn!("viewer serialize failed: {e}"),
            }
        }
        let _ = sink.close().await;
    });
    tx
}

async fn handle_join(state: Arc<ServerState>, session_id: SessionId, socket: WebSocket) {
    let (sink, mut stream) = socket.split();
    let viewer_tx = spawn_writer(sink);

    // First message must be Initialize.
    let init = loop {
        match stream.next().await {
            Some(Ok(Message::Text(t))) => match UpstreamMessage::from_json(&t) {
                Ok(UpstreamMessage::Initialize(p)) => break p,
                Ok(_) => continue,
                Err(e) => {
                    tracing::warn!("viewer init parse error: {e}");
                    return;
                }
            },
            Some(Ok(Message::Close(_))) | None => return,
            Some(Ok(_)) => continue,
            Some(Err(_)) => return,
        }
    };

    let Some(session) = state.get(&session_id) else {
        let _ = viewer_tx.send(DownstreamMessage::FailedToJoin {
            reason: FailedToJoinReason::SessionNotFound,
        });
        return;
    };

    // Allocate (or reuse, on reconnect) a viewer id.
    let viewer_id = init.viewer_id.clone().unwrap_or_else(ParticipantId::new);
    let is_reconnect = init.viewer_id.is_some();

    {
        let mut s = session.lock().await;

        if is_reconnect {
            // Catch the viewer up on anything missed, then confirm rejoin.
            for event in s.events_after(init.last_received_event_no) {
                let _ = viewer_tx.send(DownstreamMessage::OrderedTerminalEvent(event));
            }
            s.viewers.insert(viewer_id.clone(), viewer_tx.clone());
            let participant_list = Box::new(s.participants.clone());
            let _ = viewer_tx.send(DownstreamMessage::RejoinedSuccessfully { participant_list });
        } else {
            let replica = s.input_replica_id.clone();
            s.add_present_viewer(viewer_id.clone(), replica);
            s.viewers.insert(viewer_id.clone(), viewer_tx.clone());

            #[allow(deprecated)]
            let joined = DownstreamMessage::JoinedSuccessfully {
                scrollback: Box::new(s.scrollback.clone()),
                active_prompt: s.active_prompt.clone(),
                latest_event_no: s.latest_event_no,
                window_size: s.window_size,
                participant_list: Box::new(s.participants.clone()),
                viewer_id: viewer_id.clone(),
                viewer_firebase_uid: format!("self-hosted-viewer-{viewer_id}"),
                init_block_id: s.init_block_id.clone(),
                input_replica_id: s.input_replica_id.clone(),
                universal_developer_input_context: None,
                source_type: (&s.source_type).into(),
                detailed_source_type: s.source_type.clone(),
                source_task_id: s.source_task_id.clone(),
            };
            let _ = viewer_tx.send(joined);

            // Notify the sharer + other viewers that the roster changed.
            let list = s.participants.clone();
            s.send_sharer(sharer::DownstreamMessage::ParticipantListUpdated(list.clone()));
            s.broadcast_viewers(DownstreamMessage::ParticipantListUpdated(list));
        }
    }

    tracing::info!(%session_id, %viewer_id, reconnect = is_reconnect, "viewer joined");
    run_viewer_loop(state, session, session_id, viewer_id, stream).await;
}

async fn run_viewer_loop(
    state: Arc<ServerState>,
    session: Arc<Mutex<Session>>,
    session_id: SessionId,
    viewer_id: ParticipantId,
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
                tracing::warn!("viewer msg parse error: {e}");
                continue;
            }
        };

        let mut s = session.lock().await;
        match msg {
            UpstreamMessage::Initialize(_) => {}
            UpstreamMessage::Ping { data } => {
                if let Some(tx) = s.viewers.get(&viewer_id) {
                    let _ = tx.send(DownstreamMessage::Pong { data });
                }
            }
            // --- Control path: forward viewer request to the sharer ---
            UpstreamMessage::ExecuteCommand { buffer_id, command } => {
                s.send_sharer(sharer::DownstreamMessage::CommandExecutionRequested {
                    id: CommandExecutionRequestId::new(),
                    participant_id: viewer_id.clone(),
                    buffer_id,
                    command,
                });
            }
            UpstreamMessage::WriteToPty { request_id, bytes } => {
                s.send_sharer(sharer::DownstreamMessage::WriteToPtyRequested {
                    id: request_id,
                    bytes,
                });
            }
            UpstreamMessage::SendAgentPrompt(request) => {
                s.send_sharer(sharer::DownstreamMessage::AgentPromptRequested {
                    id: request.id.clone(),
                    participant_id: viewer_id.clone(),
                    request,
                });
            }
            UpstreamMessage::SendControlAction(action) => {
                s.send_sharer(sharer::DownstreamMessage::ControlActionRequested {
                    participant_id: viewer_id.clone(),
                    request_id: session_sharing_protocol::common::ControlActionRequestId::new(),
                    action,
                });
            }
            UpstreamMessage::ReportTerminalSize { window_size } => {
                s.send_sharer(sharer::DownstreamMessage::ViewerTerminalSizeReported {
                    participant_id: viewer_id.clone(),
                    window_size,
                });
            }
            UpstreamMessage::UpdateInput(update) => {
                // Optimistically applied on the viewer; echo to sharer + others.
                s.send_sharer(sharer::DownstreamMessage::InputUpdated(update.clone()));
                let me = viewer_id.clone();
                s.viewers
                    .retain(|id, tx| id == &me || tx.send(DownstreamMessage::InputUpdated(update.clone())).is_ok());
            }
            UpstreamMessage::UpdateSelection(update) => {
                let presence = DownstreamMessage::ParticipantPresenceUpdated(
                    session_sharing_protocol::common::ParticipantPresenceUpdate {
                        participant_id: viewer_id.clone(),
                        update: PresenceUpdate::Selection(update.selection.clone()),
                    },
                );
                let me = viewer_id.clone();
                s.viewers
                    .retain(|id, tx| id == &me || tx.send(presence.clone()).is_ok());
            }
            UpstreamMessage::RequestRole(role) => {
                // No limits: auto-approve any role request immediately.
                if let Some(tx) = s.viewers.get(&viewer_id) {
                    let _ = tx.send(DownstreamMessage::RoleRequestResponse(
                        session_sharing_protocol::common::RoleRequestResponse::Approved {
                            new_role: role,
                        },
                    ));
                }
                s.broadcast_viewers(DownstreamMessage::ParticipantRoleChanged {
                    participant_id: viewer_id.clone(),
                    reason: Default::default(),
                    role,
                });
            }
            UpstreamMessage::CancelRoleRequest(_)
            | UpstreamMessage::Reauthenticated { .. }
            | UpstreamMessage::UpdateUniversalDeveloperInputContext(_)
            | UpstreamMessage::UpdateLinkAccessLevel { .. }
            | UpstreamMessage::UpdateTeamAccessLevel { .. }
            | UpstreamMessage::AddGuests { .. }
            | UpstreamMessage::RemoveGuest { .. }
            | UpstreamMessage::RemovePendingGuest { .. }
            | UpstreamMessage::UpdateUserRole { .. }
            | UpstreamMessage::UpdatePendingUserRole { .. } => {
                // No-op: no ACL/guest enforcement in a limit-free relay.
            }
        }
    }

    // Viewer disconnected: drop from roster and notify others.
    {
        let mut s = session.lock().await;
        s.viewers.remove(&viewer_id);
        s.remove_present_viewer(&viewer_id);
        let list = s.participants.clone();
        s.send_sharer(sharer::DownstreamMessage::ParticipantListUpdated(list.clone()));
        s.broadcast_viewers(DownstreamMessage::ParticipantListUpdated(list));
    }
    let _ = state;
    tracing::info!(%session_id, %viewer_id, "viewer left");
}
