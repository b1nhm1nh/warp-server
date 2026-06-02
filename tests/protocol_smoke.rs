//! Serde round-trip smoke tests: prove our message construction matches the
//! pinned protocol crate's wire format.

use session_sharing_protocol::common::{OrderedTerminalEvent, OrderedTerminalEventType};
use session_sharing_protocol::sharer::UpstreamMessage as SharerUp;
use session_sharing_protocol::viewer::{DownstreamMessage as ViewerDown, FailedToJoinReason};

#[test]
fn sharer_ordered_event_round_trips() {
    let msg = SharerUp::OrderedTerminalEvent(OrderedTerminalEvent {
        event_no: 42,
        event_type: OrderedTerminalEventType::PtyBytesRead {
            bytes: b"abc".to_vec(),
        },
    });
    let json = msg.to_json().unwrap();
    let back = SharerUp::from_json(&json).unwrap();
    match back {
        SharerUp::OrderedTerminalEvent(e) => {
            assert_eq!(e.event_no, 42);
            match e.event_type {
                OrderedTerminalEventType::PtyBytesRead { bytes } => assert_eq!(bytes, b"abc"),
                _ => panic!("wrong event type"),
            }
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn viewer_failed_to_join_round_trips() {
    let msg = ViewerDown::FailedToJoin {
        reason: FailedToJoinReason::SessionNotFound,
    };
    let json = msg.to_json().unwrap();
    let back = ViewerDown::from_json(&json).unwrap();
    assert!(matches!(
        back,
        ViewerDown::FailedToJoin {
            reason: FailedToJoinReason::SessionNotFound
        }
    ));
}
