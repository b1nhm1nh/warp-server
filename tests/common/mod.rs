//! Shared test harness: spawn the real `warp-server` binary on a free port and
//! drive it with real WebSocket clients (tokio-tungstenite). Zero src coupling.

use std::net::TcpListener as StdTcpListener;
use std::process::{Child, Command};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use session_sharing_protocol::common::{
    ActivePrompt, BlockId, Scrollback, ScrollbackBlock, Selection, UserID, WindowSize,
};
use session_sharing_protocol::sharer::{self, InitPayload as SharerInit};
use session_sharing_protocol::viewer::{self, InitPayload as ViewerInit};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tungstenite::Message;

pub type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// A spawned server process that is killed when dropped.
pub struct TestServer {
    child: Child,
    pub port: u16,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl TestServer {
    pub async fn start() -> Self {
        Self::start_with_args(&[]).await
    }

    /// Like [`TestServer::start`] but appends `extra` CLI args (e.g.
    /// `&["--session-grace-secs", "1"]`) to the spawned binary.
    pub async fn start_with_args(extra: &[&str]) -> Self {
        // Grab a free port by binding to :0 then releasing it.
        let port = {
            let l = StdTcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let child = Command::new(env!("CARGO_BIN_EXE_warp-server"))
            .arg("--addr")
            .arg(format!("127.0.0.1:{port}"))
            .args(extra)
            .env("RUST_LOG", "warn")
            .spawn()
            .expect("spawn warp-server");

        let server = TestServer { child, port };
        server.wait_ready().await;
        server
    }

    async fn wait_ready(&self) {
        for _ in 0..100 {
            if TcpStream::connect(("127.0.0.1", self.port)).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("server did not become ready on port {}", self.port);
    }

    pub fn url(&self, path: &str) -> String {
        format!("ws://127.0.0.1:{}{path}", self.port)
    }
}

pub async fn connect(url: &str) -> Ws {
    let (ws, _resp) = connect_async(url).await.expect("ws connect");
    ws
}

pub async fn send_sharer(ws: &mut Ws, msg: &sharer::UpstreamMessage) {
    ws.send(Message::Text(msg.to_json().unwrap())).await.unwrap();
}

pub async fn send_viewer(ws: &mut Ws, msg: &viewer::UpstreamMessage) {
    ws.send(Message::Text(msg.to_json().unwrap())).await.unwrap();
}

/// Read frames until one parses as a sharer DownstreamMessage matching `pred`.
pub async fn recv_sharer<F>(ws: &mut Ws, pred: F) -> sharer::DownstreamMessage
where
    F: Fn(&sharer::DownstreamMessage) -> bool,
{
    let fut = async {
        while let Some(frame) = ws.next().await {
            if let Ok(Message::Text(t)) = frame
                && let Ok(m) = sharer::DownstreamMessage::from_json(&t)
                && pred(&m)
            {
                return m;
            }
        }
        panic!("sharer stream ended before a matching message");
    };
    tokio::time::timeout(Duration::from_secs(5), fut)
        .await
        .expect("timed out waiting for sharer message")
}

/// Read frames until one parses as a viewer DownstreamMessage matching `pred`.
pub async fn recv_viewer<F>(ws: &mut Ws, pred: F) -> viewer::DownstreamMessage
where
    F: Fn(&viewer::DownstreamMessage) -> bool,
{
    let fut = async {
        while let Some(frame) = ws.next().await {
            if let Ok(Message::Text(t)) = frame
                && let Ok(m) = viewer::DownstreamMessage::from_json(&t)
                && pred(&m)
            {
                return m;
            }
        }
        panic!("viewer stream ended before a matching message");
    };
    tokio::time::timeout(Duration::from_secs(5), fut)
        .await
        .expect("timed out waiting for viewer message")
}

/// Assert that NO viewer message matching `pred` arrives within `ms`.
// Shared harness helper: not every test binary uses every helper.
#[allow(dead_code)]
pub async fn assert_no_viewer<F>(ws: &mut Ws, ms: u64, pred: F)
where
    F: Fn(&viewer::DownstreamMessage) -> bool,
{
    let fut = async {
        while let Some(frame) = ws.next().await {
            if let Ok(Message::Text(t)) = frame
                && let Ok(m) = viewer::DownstreamMessage::from_json(&t)
                && pred(&m)
            {
                panic!("unexpected matching viewer message arrived");
            }
        }
    };
    let _ = tokio::time::timeout(Duration::from_millis(ms), fut).await;
}

// ---- payload builders ----

pub fn scrollback_with(bytes: &[u8]) -> Scrollback {
    Scrollback {
        blocks: vec![ScrollbackBlock {
            raw: bytes.to_vec(),
        }],
        is_alt_screen_active: false,
    }
}

pub fn sharer_init(scrollback: Scrollback) -> sharer::UpstreamMessage {
    sharer::UpstreamMessage::Initialize(SharerInit {
        scrollback,
        active_prompt: ActivePrompt::default(),
        window_size: WindowSize::default(),
        user_id: UserID::default(),
        selection: Selection::default(),
        init_block_id: BlockId::default(),
        input_replica_id: Default::default(),
        telemetry_context: None,
        lifetime: Default::default(),
        universal_developer_input_context: None,
        source_type: Default::default(),
        source_task_id: None,
        feature_support: Default::default(),
    })
}

pub fn viewer_init(
    viewer_id: Option<session_sharing_protocol::common::ParticipantId>,
    last_received_event_no: Option<usize>,
) -> viewer::UpstreamMessage {
    viewer::UpstreamMessage::Initialize(ViewerInit {
        viewer_id,
        user_id: UserID::default(),
        last_received_event_no,
        latest_block_id: None,
        telemetry_context: None,
        feature_support: Default::default(),
    })
}

// Shared harness helper: not every test binary uses every helper.
#[allow(dead_code)]
pub fn reconnect_msg(token: sharer::ReconnectToken) -> sharer::UpstreamMessage {
    use session_sharing_protocol::common::SessionSecret;
    sharer::UpstreamMessage::Reconnect(sharer::ReconnectPayload {
        session_secret: SessionSecret::default(),
        reconnect_token: token,
        user_id: UserID::default(),
        latest_block_id: BlockId::default(),
        selection: Selection::default(),
        feature_support: Default::default(),
    })
}

pub fn pty_event(event_no: usize, bytes: &[u8]) -> sharer::UpstreamMessage {
    use session_sharing_protocol::common::{OrderedTerminalEvent, OrderedTerminalEventType};
    sharer::UpstreamMessage::OrderedTerminalEvent(OrderedTerminalEvent {
        event_no,
        event_type: OrderedTerminalEventType::PtyBytesRead {
            bytes: bytes.to_vec(),
        },
    })
}
