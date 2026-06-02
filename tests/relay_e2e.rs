//! End-to-end relay tests: real binary, real WebSocket clients.

mod common;

use common::*;
use session_sharing_protocol::common::{OrderedTerminalEventType, SessionId};
use session_sharing_protocol::sharer::DownstreamMessage as SharerDown;
use session_sharing_protocol::viewer::{
    DownstreamMessage as ViewerDown, FailedToJoinReason, UpstreamMessage as ViewerUp,
};

/// Helper: open a sharer session, returning (ws, session_id).
async fn open_session(server: &TestServer, scrollback_bytes: &[u8]) -> (Ws, SessionId) {
    let mut ws = connect(&server.url("/sessions/create")).await;
    send_sharer(&mut ws, &sharer_init(scrollback_with(scrollback_bytes))).await;
    let init = recv_sharer(&mut ws, |m| matches!(m, SharerDown::SessionInitialized { .. })).await;
    let SharerDown::SessionInitialized { session_id, .. } = init else {
        unreachable!()
    };
    (ws, session_id)
}

/// Helper: join a viewer, returning (ws, joined_message).
async fn join_viewer(server: &TestServer, id: &SessionId) -> (Ws, ViewerDown) {
    let mut ws = connect(&server.url(&format!("/sessions/join/{id}"))).await;
    send_viewer(&mut ws, &viewer_init(None, None)).await;
    let joined = recv_viewer(&mut ws, |m| matches!(m, ViewerDown::JoinedSuccessfully { .. })).await;
    (ws, joined)
}

#[tokio::test]
async fn create_join_and_scrollback() {
    let server = TestServer::start().await;
    let (_sharer, id) = open_session(&server, b"history-bytes").await;
    let (_viewer, joined) = join_viewer(&server, &id).await;

    let ViewerDown::JoinedSuccessfully { scrollback, .. } = joined else {
        panic!("expected JoinedSuccessfully");
    };
    assert_eq!(scrollback.blocks.len(), 1);
    assert_eq!(scrollback.blocks[0].raw, b"history-bytes");
}

#[tokio::test]
async fn live_fanout_to_two_viewers_and_ack() {
    let server = TestServer::start().await;
    let (mut sharer, id) = open_session(&server, b"").await;
    let (mut v1, _) = join_viewer(&server, &id).await;
    let (mut v2, _) = join_viewer(&server, &id).await;

    send_sharer(&mut sharer, &pty_event(0, b"hello")).await;

    for v in [&mut v1, &mut v2] {
        let ev = recv_viewer(v, |m| matches!(m, ViewerDown::OrderedTerminalEvent(_))).await;
        let ViewerDown::OrderedTerminalEvent(e) = ev else {
            unreachable!()
        };
        match e.event_type {
            OrderedTerminalEventType::PtyBytesRead { bytes } => assert_eq!(bytes, b"hello"),
            _ => panic!("wrong event type"),
        }
    }

    // Sharer should be acked.
    let ack = recv_sharer(&mut sharer, |m| {
        matches!(m, SharerDown::EventsProcessedAck { .. })
    })
    .await;
    let SharerDown::EventsProcessedAck {
        latest_processed_event_no,
    } = ack
    else {
        unreachable!()
    };
    assert_eq!(latest_processed_event_no, 0);
}

#[tokio::test]
async fn fresh_join_reports_latest_event_no_and_reconnect_catches_up() {
    let server = TestServer::start().await;
    let (mut sharer, id) = open_session(&server, b"").await;

    // Three events before anyone joins.
    for n in 0..3 {
        send_sharer(&mut sharer, &pty_event(n, format!("e{n}").as_bytes())).await;
    }
    // Drain sharer acks so they don't pile up (best-effort).
    let _ = recv_sharer(&mut sharer, |m| {
        matches!(m, SharerDown::EventsProcessedAck { latest_processed_event_no } if *latest_processed_event_no == 2)
    })
    .await;

    // Fresh viewer: no event replay, but latest_event_no must be Some(2).
    let (viewer, joined) = join_viewer(&server, &id).await;
    let (viewer_id, latest) = match joined {
        ViewerDown::JoinedSuccessfully {
            viewer_id,
            latest_event_no,
            ..
        } => (viewer_id, latest_event_no),
        _ => unreachable!(),
    };
    assert_eq!(latest, Some(2));

    // Disconnect, then reconnect with last_received_event_no = Some(0): expect
    // catch-up of events 1 and 2, then RejoinedSuccessfully.
    drop(viewer);
    let mut rec = connect(&server.url(&format!("/sessions/join/{id}"))).await;
    send_viewer(&mut rec, &viewer_init(Some(viewer_id), Some(0))).await;

    let e1 = recv_viewer(&mut rec, |m| matches!(m, ViewerDown::OrderedTerminalEvent(_))).await;
    let ViewerDown::OrderedTerminalEvent(e1) = e1 else {
        unreachable!()
    };
    assert_eq!(e1.event_no, 1);

    let e2 = recv_viewer(&mut rec, |m| {
        matches!(m, ViewerDown::OrderedTerminalEvent(e) if e.event_no == 2)
    })
    .await;
    let ViewerDown::OrderedTerminalEvent(e2) = e2 else {
        unreachable!()
    };
    assert_eq!(e2.event_no, 2);

    let _ = recv_viewer(&mut rec, |m| {
        matches!(m, ViewerDown::RejoinedSuccessfully { .. })
    })
    .await;
}

#[tokio::test]
async fn viewer_execute_command_forwarded_to_sharer() {
    let server = TestServer::start().await;
    let (mut sharer, id) = open_session(&server, b"").await;
    let (mut viewer, joined) = join_viewer(&server, &id).await;
    let viewer_id = match joined {
        ViewerDown::JoinedSuccessfully { viewer_id, .. } => viewer_id,
        _ => unreachable!(),
    };

    send_viewer(
        &mut viewer,
        &ViewerUp::ExecuteCommand {
            buffer_id: Default::default(),
            command: "ls".to_owned(),
        },
    )
    .await;

    let req = recv_sharer(&mut sharer, |m| {
        matches!(m, SharerDown::CommandExecutionRequested { .. })
    })
    .await;
    let SharerDown::CommandExecutionRequested {
        participant_id,
        command,
        ..
    } = req
    else {
        unreachable!()
    };
    assert_eq!(command, "ls");
    assert_eq!(participant_id, viewer_id);
}

#[tokio::test]
async fn two_sessions_do_not_cross_talk() {
    let server = TestServer::start().await;
    let (mut sharer_a, id_a) = open_session(&server, b"").await;
    let (mut sharer_b, id_b) = open_session(&server, b"").await;
    assert_ne!(id_a.to_string(), id_b.to_string());

    let (mut va, _) = join_viewer(&server, &id_a).await;
    let (mut vb, _) = join_viewer(&server, &id_b).await;

    send_sharer(&mut sharer_a, &pty_event(0, b"AAA")).await;
    send_sharer(&mut sharer_b, &pty_event(0, b"BBB")).await;

    let ea = recv_viewer(&mut va, |m| matches!(m, ViewerDown::OrderedTerminalEvent(_))).await;
    let ViewerDown::OrderedTerminalEvent(ea) = ea else {
        unreachable!()
    };
    match ea.event_type {
        OrderedTerminalEventType::PtyBytesRead { bytes } => assert_eq!(bytes, b"AAA"),
        _ => panic!("wrong type"),
    }
    // Viewer A must NOT receive BBB.
    assert_no_viewer(&mut va, 300, |m| {
        matches!(m, ViewerDown::OrderedTerminalEvent(e)
            if matches!(&e.event_type, OrderedTerminalEventType::PtyBytesRead { bytes } if bytes == b"BBB"))
    })
    .await;

    let eb = recv_viewer(&mut vb, |m| matches!(m, ViewerDown::OrderedTerminalEvent(_))).await;
    let ViewerDown::OrderedTerminalEvent(eb) = eb else {
        unreachable!()
    };
    match eb.event_type {
        OrderedTerminalEventType::PtyBytesRead { bytes } => assert_eq!(bytes, b"BBB"),
        _ => panic!("wrong type"),
    }
}

#[tokio::test]
async fn join_nonexistent_session_fails() {
    let server = TestServer::start().await;
    let random = SessionId::new();
    let mut ws = connect(&server.url(&format!("/sessions/join/{random}"))).await;
    send_viewer(&mut ws, &viewer_init(None, None)).await;

    let failed = recv_viewer(&mut ws, |m| matches!(m, ViewerDown::FailedToJoin { .. })).await;
    let ViewerDown::FailedToJoin { reason } = failed else {
        unreachable!()
    };
    assert!(matches!(reason, FailedToJoinReason::SessionNotFound));
}
