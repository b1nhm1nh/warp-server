//! Security regression tests. These prove three fixes hold via the black-box
//! WebSocket interface, driving the real binary through the shared harness:
//!   * #1 — sharer `/resume` requires the correct `reconnect_token`.
//!   * #3 — sessions are reaped after the sharer disconnects (grace period).
//!   * #4 — the relay stays functionally correct under a burst of events.

mod common;

use common::*;
use session_sharing_protocol::common::{OrderedTerminalEventType, SessionId};
use session_sharing_protocol::sharer::{
    DownstreamMessage as SharerDown, ReconnectToken, ReconnectionFailedReason,
};
use session_sharing_protocol::viewer::{
    DownstreamMessage as ViewerDown, FailedToJoinReason, SessionEndedReason,
};

/// Open a sharer session, returning (ws, session_id, reconnect_token).
async fn open_session(server: &TestServer, scrollback_bytes: &[u8]) -> (Ws, SessionId, ReconnectToken) {
    let mut ws = connect(&server.url("/sessions/create")).await;
    send_sharer(&mut ws, &sharer_init(scrollback_with(scrollback_bytes))).await;
    let init = recv_sharer(&mut ws, |m| matches!(m, SharerDown::SessionInitialized { .. })).await;
    let SharerDown::SessionInitialized {
        session_id,
        reconnect_token,
        ..
    } = init
    else {
        unreachable!()
    };
    (ws, session_id, reconnect_token)
}

/// Join a viewer; assert it joined successfully and return (ws, joined_message).
async fn join_viewer(server: &TestServer, id: &SessionId) -> (Ws, ViewerDown) {
    let mut ws = connect(&server.url(&format!("/sessions/join/{id}"))).await;
    send_viewer(&mut ws, &viewer_init(None, None)).await;
    let joined =
        recv_viewer(&mut ws, |m| matches!(m, ViewerDown::JoinedSuccessfully { .. })).await;
    (ws, joined)
}

/// #1: resuming with a token that does not match the one issued at creation is
/// rejected with `WrongReconnectionToken`. Without this check anyone who learns
/// the session_id could hijack the sharer side.
#[tokio::test]
async fn resume_with_wrong_token_is_rejected() {
    let server = TestServer::start().await;
    let (_sharer, id, _token) = open_session(&server, b"").await;

    let mut rec = connect(&server.url(&format!("/sessions/{id}/resume"))).await;
    // A fresh (bogus) token, NOT the one issued in SessionInitialized.
    send_sharer(&mut rec, &reconnect_msg(ReconnectToken::new())).await;

    let failed = recv_sharer(&mut rec, |m| matches!(m, SharerDown::FailedToReconnect { .. })).await;
    let SharerDown::FailedToReconnect { reason } = failed else {
        unreachable!()
    };
    assert!(
        matches!(reason, ReconnectionFailedReason::WrongReconnectionToken),
        "expected WrongReconnectionToken, got {reason:?}"
    );
}

/// #1 (positive case): resuming with the correct captured token succeeds.
#[tokio::test]
async fn resume_with_correct_token_succeeds() {
    let server = TestServer::start().await;
    let (sharer, id, token) = open_session(&server, b"").await;

    // Simulate the sharer's socket dropping.
    drop(sharer);

    let mut rec = connect(&server.url(&format!("/sessions/{id}/resume"))).await;
    send_sharer(&mut rec, &reconnect_msg(token)).await;

    let _ = recv_sharer(&mut rec, |m| matches!(m, SharerDown::SessionReconnected { .. })).await;
}

/// #3: after the sharer disconnects and the grace period elapses with no
/// `/resume`, the session is reaped: viewers get `SessionEnded { EndedBySharer }`
/// and a later join returns `FailedToJoin { SessionNotFound }`.
#[tokio::test]
async fn session_reaped_after_sharer_disconnect() {
    let server = TestServer::start_with_args(&["--session-grace-secs", "1"]).await;
    let (sharer, id, _token) = open_session(&server, b"").await;
    let (mut viewer, _joined) = join_viewer(&server, &id).await;

    // Sharer drops without EndSession; reaper should fire after ~1s.
    drop(sharer);

    let ended = recv_viewer(&mut viewer, |m| matches!(m, ViewerDown::SessionEnded { .. })).await;
    let ViewerDown::SessionEnded { reason } = ended else {
        unreachable!()
    };
    assert!(
        matches!(reason, SessionEndedReason::EndedBySharer),
        "expected EndedBySharer, got {reason:?}"
    );

    // A fresh viewer joining the (now-removed) session must be told it's gone.
    let mut late = connect(&server.url(&format!("/sessions/join/{id}"))).await;
    send_viewer(&mut late, &viewer_init(None, None)).await;
    let failed = recv_viewer(&mut late, |m| matches!(m, ViewerDown::FailedToJoin { .. })).await;
    let ViewerDown::FailedToJoin { reason } = failed else {
        unreachable!()
    };
    assert!(
        matches!(reason, FailedToJoinReason::SessionNotFound),
        "expected SessionNotFound, got {reason:?}"
    );
}

/// #3 (within-grace case): resuming before the grace period elapses keeps the
/// session alive — it is NOT reaped, and viewers can still join.
#[tokio::test]
async fn session_survives_within_grace_then_resumes() {
    let server = TestServer::start_with_args(&["--session-grace-secs", "3"]).await;
    let (sharer, id, token) = open_session(&server, b"").await;

    drop(sharer);
    // Resume well within the 3s grace window.
    let mut rec = connect(&server.url(&format!("/sessions/{id}/resume"))).await;
    send_sharer(&mut rec, &reconnect_msg(token)).await;
    let _ = recv_sharer(&mut rec, |m| matches!(m, SharerDown::SessionReconnected { .. })).await;

    // Past the original grace window: the session must still exist.
    tokio::time::sleep(std::time::Duration::from_millis(3500)).await;
    let _viewer = join_viewer(&server, &id).await; // recv asserts JoinedSuccessfully
}

/// #4 (lightweight functional check): a burst of events is relayed in order to a
/// viewer, and a late reconnecting viewer catches up. Confirms the bounded
/// event-log ring buffer / writer channels don't drop correctness under load.
/// Kept small (300 events) to stay fast.
#[tokio::test]
async fn many_events_still_deliver() {
    const N: usize = 300;
    let server = TestServer::start().await;
    let (mut sharer, id, _token) = open_session(&server, b"").await;
    let (mut viewer, joined) = join_viewer(&server, &id).await;
    let ViewerDown::JoinedSuccessfully { viewer_id, .. } = joined else {
        unreachable!()
    };

    for n in 0..N {
        send_sharer(&mut sharer, &pty_event(n, format!("e{n}").as_bytes())).await;
    }

    // The live viewer must receive the final event in order.
    let last = recv_viewer(&mut viewer, |m| {
        matches!(m, ViewerDown::OrderedTerminalEvent(e) if e.event_no == N - 1)
    })
    .await;
    let ViewerDown::OrderedTerminalEvent(e) = last else {
        unreachable!()
    };
    match e.event_type {
        OrderedTerminalEventType::PtyBytesRead { bytes } => {
            assert_eq!(bytes, format!("e{}", N - 1).as_bytes());
        }
        _ => panic!("wrong event type"),
    }

    // A reconnecting viewer (same viewer_id) from event 0 catches up to latest.
    drop(viewer);
    let mut late = connect(&server.url(&format!("/sessions/join/{id}"))).await;
    send_viewer(&mut late, &viewer_init(Some(viewer_id), Some(0))).await;
    let _ = recv_viewer(&mut late, |m| {
        matches!(m, ViewerDown::OrderedTerminalEvent(e) if e.event_no == N - 1)
    })
    .await;
}
