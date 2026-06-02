//! Live multi-session demo: drives a RUNNING warp-server with real WebSocket
//! clients to prove many concurrent remote-control sessions work with no limits.
//!
//! Usage:
//!   cargo run --example multi_session_demo -- [ws://127.0.0.1:8787] [num_sessions]
//!
//! For each session it opens 1 sharer + 2 viewers, streams pty bytes from the
//! sharer, has a viewer issue a remote command, and prints what each party sees.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use session_sharing_protocol::common::{
    ActivePrompt, BlockId, OrderedTerminalEvent, OrderedTerminalEventType, Scrollback,
    ScrollbackBlock, Selection, SessionId, UserID, WindowSize,
};
use session_sharing_protocol::sharer::{self, InitPayload as SharerInit};
use session_sharing_protocol::viewer::{self, InitPayload as ViewerInit};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tungstenite::Message;

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let base = args.next().unwrap_or_else(|| "ws://127.0.0.1:8787".to_owned());
    let n: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(3);

    println!("== driving {base} with {n} concurrent sessions ==\n");

    // Launch all sessions concurrently to prove no global serialization / limit.
    let mut handles = Vec::new();
    for i in 0..n {
        let base = base.clone();
        handles.push(tokio::spawn(async move { run_session(&base, i).await }));
    }

    let mut ok = 0;
    for (i, h) in handles.into_iter().enumerate() {
        match h.await? {
            Ok(()) => {
                ok += 1;
            }
            Err(e) => println!("[session {i}] FAILED: {e:#}"),
        }
    }

    println!("\n== {ok}/{n} sessions succeeded (no quota / limit errors) ==");
    if ok == n {
        Ok(())
    } else {
        anyhow::bail!("{}/{} sessions failed", n - ok, n)
    }
}

async fn run_session(base: &str, idx: usize) -> anyhow::Result<()> {
    let tag = format!("session{idx}");
    let payload = format!("output-from-{tag}");

    // --- sharer creates the session ---
    let mut sharer = connect(&format!("{base}/sessions/create")).await?;
    let scrollback = Scrollback {
        blocks: vec![ScrollbackBlock {
            raw: format!("scrollback-{tag}").into_bytes(),
        }],
        is_alt_screen_active: false,
    };
    send(&mut sharer, &sharer_init(scrollback)).await?;
    let session_id = match recv_sharer(&mut sharer).await? {
        sharer::DownstreamMessage::SessionInitialized { session_id, .. } => session_id,
        _ => anyhow::bail!("expected SessionInitialized as first sharer message"),
    };
    println!("[{tag}] sharer created session {session_id}");

    // --- two viewers join (proves >1 concurrent viewer, no maxConcurrentViewers) ---
    let mut v1 = join(base, &session_id).await?;
    let v1_id = expect_joined(&mut v1, &tag, "viewer1").await?;
    let mut v2 = join(base, &session_id).await?;
    let _v2_id = expect_joined(&mut v2, &tag, "viewer2").await?;

    // --- sharer streams a live pty event; both viewers must receive it ---
    send(
        &mut sharer,
        &sharer::UpstreamMessage::OrderedTerminalEvent(OrderedTerminalEvent {
            event_no: 0,
            event_type: OrderedTerminalEventType::PtyBytesRead {
                bytes: payload.clone().into_bytes(),
            },
        }),
    )
    .await?;

    for (name, v) in [("viewer1", &mut v1), ("viewer2", &mut v2)] {
        let bytes = expect_pty(v).await?;
        let got = String::from_utf8_lossy(&bytes);
        assert_eq!(got, payload, "[{tag}] {name} saw wrong bytes");
        println!("[{tag}] {name} received live output: {got:?}");
    }

    // --- sharer is acked ---
    if let sharer::DownstreamMessage::EventsProcessedAck {
        latest_processed_event_no,
    } = recv_sharer(&mut sharer).await?
    {
        println!("[{tag}] sharer acked through event {latest_processed_event_no}");
    }

    // --- REMOTE CONTROL: viewer1 runs a command, sharer must receive it ---
    send(
        &mut v1,
        &viewer::UpstreamMessage::ExecuteCommand {
            buffer_id: Default::default(),
            command: format!("echo {tag}"),
        },
    )
    .await?;
    loop {
        match recv_sharer(&mut sharer).await? {
            sharer::DownstreamMessage::CommandExecutionRequested {
                participant_id,
                command,
                ..
            } => {
                assert_eq!(participant_id, v1_id);
                println!("[{tag}] sharer got remote command from viewer1: {command:?}");
                break;
            }
            // ignore roster/presence chatter
            _ => continue,
        }
    }

    println!("[{tag}] OK (1 sharer + 2 viewers, live output + remote control)");
    Ok(())
}

// ---- helpers ----

async fn connect(url: &str) -> anyhow::Result<Ws> {
    let (ws, _) = connect_async(url).await?;
    Ok(ws)
}

async fn join(base: &str, id: &SessionId) -> anyhow::Result<Ws> {
    let mut ws = connect(&format!("{base}/sessions/join/{id}")).await?;
    send(&mut ws, &viewer_init(None, None)).await?;
    Ok(ws)
}

async fn send<T>(ws: &mut Ws, msg: &T) -> anyhow::Result<()>
where
    T: Serializable,
{
    ws.send(Message::Text(msg.to_json()?)).await?;
    Ok(())
}

trait Serializable {
    fn to_json(&self) -> serde_json::Result<String>;
}
impl Serializable for sharer::UpstreamMessage {
    fn to_json(&self) -> serde_json::Result<String> {
        sharer::UpstreamMessage::to_json(self)
    }
}
impl Serializable for viewer::UpstreamMessage {
    fn to_json(&self) -> serde_json::Result<String> {
        viewer::UpstreamMessage::to_json(self)
    }
}

async fn recv_sharer(ws: &mut Ws) -> anyhow::Result<sharer::DownstreamMessage> {
    let fut = async {
        while let Some(frame) = ws.next().await {
            if let Ok(Message::Text(t)) = frame
                && let Ok(m) = sharer::DownstreamMessage::from_json(&t)
            {
                return Ok(m);
            }
        }
        anyhow::bail!("sharer stream ended")
    };
    tokio::time::timeout(Duration::from_secs(5), fut).await?
}

async fn recv_viewer(ws: &mut Ws) -> anyhow::Result<viewer::DownstreamMessage> {
    let fut = async {
        while let Some(frame) = ws.next().await {
            if let Ok(Message::Text(t)) = frame
                && let Ok(m) = viewer::DownstreamMessage::from_json(&t)
            {
                return Ok(m);
            }
        }
        anyhow::bail!("viewer stream ended")
    };
    tokio::time::timeout(Duration::from_secs(5), fut).await?
}

async fn expect_joined(
    ws: &mut Ws,
    tag: &str,
    name: &str,
) -> anyhow::Result<session_sharing_protocol::common::ParticipantId> {
    loop {
        if let viewer::DownstreamMessage::JoinedSuccessfully {
            viewer_id,
            scrollback,
            ..
        } = recv_viewer(ws).await?
        {
            println!(
                "[{tag}] {name} joined (got {} scrollback block(s))",
                scrollback.blocks.len()
            );
            return Ok(viewer_id);
        }
    }
}

async fn expect_pty(ws: &mut Ws) -> anyhow::Result<Vec<u8>> {
    loop {
        if let viewer::DownstreamMessage::OrderedTerminalEvent(e) = recv_viewer(ws).await?
            && let OrderedTerminalEventType::PtyBytesRead { bytes } = e.event_type
        {
            return Ok(bytes);
        }
    }
}

fn sharer_init(scrollback: Scrollback) -> sharer::UpstreamMessage {
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

fn viewer_init(
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
