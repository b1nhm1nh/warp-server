# warp-server

A self-hosted, **limit-free** relay for Warp's session sharing ("remote control").

Warp's hosted relay (`wss://sessions.app.warp.dev`) enforces quotas — a daily
session `limit`, `maxConcurrentViewers`, and `NoUserQuotaRemaining` — that block
sharing multiple sessions. Those limits live only in the closed hosted server.
The **wire protocol is public** (`session-sharing-protocol`), so this relay
speaks it and simply imposes nothing: any number of concurrent sessions, any
number of viewers, no daily cap. Auth tokens are accepted but ignored.

## What it does

- Single in-memory process (axum + tokio). Lowest latency; state is per-session
  and lock-free across sessions (`DashMap`).
- Relays terminal output sharer → viewers (lossless, ordered) and forwards
  control requests viewer → sharer (execute command, write-to-pty, control
  actions, resize), so viewers can both **watch and drive** a session.
- Every viewer gets the highest role (`Full`); role requests auto-approve.
- ACL / guest / team / quota messages are intentional no-ops.
- State is in RAM only — restart drops live sessions.

## Endpoints

| Route | Who | Purpose |
|-------|-----|---------|
| `GET /sessions/create` | sharer | start a session (first WS msg: `Initialize`) |
| `GET /sessions/join/:session_id` | viewer | join (or reconnect with `viewer_id`) |
| `GET /sessions/:session_id/resume` | sharer | reconnect after a drop |
| `GET /healthz` | — | liveness probe (returns `ok`) |

Messages are JSON text frames per the `session-sharing-protocol` crate (pinned
to the exact rev the Warp client builds against, so the wire format matches).

## Run

```bash
cargo run -- --addr 127.0.0.1:8787      # or set WARP_SERVER_ADDR
# RUST_LOG=warp_server=debug for verbose logs
```

## Point the Warp client at it

By default the open-source `warp-oss` build ignores server-URL overrides
(`Channel::allows_server_url_overrides()` returns `false` for release channels),
so it always talks to `wss://sessions.app.warp.dev`. A **one-line patch** adds
`Channel::Oss` to the allowed set so the build honors
`WARP_SESSION_SHARING_SERVER_URL`. The patch (plus a unit test) lives in
[`patches/`](patches/).

Apply it to a [`warpdotdev/warp`](https://github.com/warpdotdev/warp) checkout:

```bash
cd /path/to/warp
git apply /path/to/warp-server/patches/0001-allow-oss-session-sharing-url-override.patch
# verify it took:
cargo test -p warp_core channel::          # server_url_override_allowed_per_channel passes
```

Then build/run the client pointed at this server:

```bash
WARP_SESSION_SHARING_SERVER_URL=ws://127.0.0.1:8787 ./script/run
```

In the client: `/remote-control` to share, open the link from another instance
to view/control. Share as many sessions as you like — no quota error.

> The patch is intentionally tiny and isolated to one file so it keeps applying
> cleanly as upstream moves. See [Syncing with upstream](#syncing-with-upstream).

## Test

```bash
cargo test                                  # 8 e2e + smoke tests
cargo clippy --all-targets -- -D warnings
```

`tests/relay_e2e.rs` spins up the real binary on an ephemeral port and drives it
with real WebSocket clients: scrollback delivery, live fan-out to multiple
viewers, late-join + reconnect catch-up, viewer→sharer command forwarding, and
two simultaneous sessions with no cross-talk.

### Live multi-session demo

Drive a running server with N concurrent sessions (each: 1 sharer + 2 viewers +
a remote command), to see multi-session + remote control end to end:

```bash
cargo run -- --addr 127.0.0.1:8787 &                       # start the server
cargo run --example multi_session_demo -- ws://127.0.0.1:8787 25
# => "25/25 sessions succeeded (no quota / limit errors)"
```

## Syncing with upstream

The only client-side change is the one-line patch in `patches/`. To track a
newer `warpdotdev/warp`:

1. Re-apply the patch on the updated checkout (`git apply` / `git apply -3`). If
   it conflicts, the target function `allows_server_url_overrides()` moved —
   re-add `Channel::Oss` to the `true` arm by hand and regenerate the patch.
2. Check whether the client bumped the `session-sharing-protocol` rev
   (`grep session-sharing-protocol warp/Cargo.toml`) and match it in this repo's
   `Cargo.toml`, then `cargo test` to catch protocol drift.

A repeatable checklist and an agent skill for this live under [`docs/`](docs/)
and `.claude/skills/` (`sync-upstream`).

## Notes / limitations

- Identity is placeholder (real Warp backend tokens are ignored) — avatars and
  display names are generic; view + control work regardless.
- Opaque payloads (`AgentResponseEvent`, CRDT input ops) are relayed as bytes,
  never interpreted.
- Not built for HA: one process, in-memory. For multi-machine scale you'd add a
  shared pub/sub layer (out of scope by design — latency-first).

## License

AGPL-3.0-only. This binary links the `session-sharing-protocol` crate, which is
AGPL-3.0; distributing `warp-server` therefore carries the same license. See
[`LICENSE`](LICENSE).
