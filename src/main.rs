//! Self-hosted, limit-free relay for Warp session sharing ("remote control").
//!
//! Speaks the public `session-sharing-protocol` wire format (JSON over WebSocket)
//! and imposes **no quotas**: any number of concurrent sessions, any number of
//! viewers, no daily limit. Auth tokens in the protocol are accepted but ignored.
//!
//! Three endpoints, matching what the Warp client connects to
//! (`app/src/terminal/shared_session/{sharer,viewer}/network.rs`):
//!   * `GET /sessions/create`            — sharer starts a session
//!   * `GET /sessions/join/{id}`         — viewer joins a session
//!   * `GET /sessions/{id}/resume`       — sharer reconnects to a session

mod session;
mod sharer_ws;
mod state;
mod viewer_ws;

use std::net::SocketAddr;
use std::time::Duration;

use axum::{Router, routing::get};
use clap::Parser;
use state::{Config, ServerState};
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(name = "warp-server", about = "Limit-free Warp session-sharing relay")]
struct Args {
    /// Address to bind, e.g. 127.0.0.1:8787 or 0.0.0.0:8787.
    ///
    /// SECURITY: knowing a session's UUID is sufficient to join AND remote-control
    /// the sharer's terminal (the client sends no join secret). Keep this on
    /// loopback unless you front it with TLS and trust the network.
    #[arg(long, env = "WARP_SERVER_ADDR", default_value = "127.0.0.1:8787")]
    addr: SocketAddr,

    /// Seconds to retain a session after its sharer disconnects, awaiting
    /// `/resume`. After this, the session is reaped (viewers notified).
    #[arg(long, env = "WARP_SERVER_SESSION_GRACE_SECS", default_value_t = 120)]
    session_grace_secs: u64,

    /// Max inbound WebSocket message size in bytes (per message).
    #[arg(long, env = "WARP_SERVER_MAX_MESSAGE_BYTES", default_value_t = 16 * 1024 * 1024)]
    max_message_bytes: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "warp_server=info,tower_http=warn".into()),
        )
        .init();

    let args = Args::parse();
    let state = Arc::new(ServerState::with_config(Config {
        sharer_grace: Duration::from_secs(args.session_grace_secs),
        max_message_bytes: args.max_message_bytes,
    }));

    let app = Router::new()
        .route("/sessions/create", get(sharer_ws::create))
        .route("/sessions/join/:session_id", get(viewer_ws::join))
        .route("/sessions/:session_id/resume", get(sharer_ws::resume))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(args.addr).await?;
    tracing::info!("warp-server listening on ws://{} (no limits)", args.addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutting down");
        })
        .await?;
    Ok(())
}
