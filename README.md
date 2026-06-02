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

### Quickstart

`quickstart.sh` builds and launches the relay (and optionally the client,
wired up correctly):

```bash
./quickstart.sh                      # build + run the relay (foreground)
./quickstart.sh --client             # relay + launch a sharer client (built with
                                     #   creating_shared_sessions so /remote-control shows)
./quickstart.sh --join <SESSION_ID>  # relay + launch a viewer via the warposs:// deeplink
```

It expects a `warpdotdev/warp` checkout at `../warp` (override with `--warp <path>`)
for the `--client`/`--join` modes. `--help` lists all options.

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

## Remote control: gotchas

Hard-won notes from driving the real GUI client against this relay. Most "it
doesn't work" cases are one of these, not a relay bug.

### `/remote-control` doesn't appear

It's gated behind **two** cargo features (both must be enabled), and it lives in
the **AI/agent input box**, not the shell prompt:

- `hoa_remote_control` (on by default) **and** `creating_shared_sessions`
  (**not** in the default set). Build with both:
  ```bash
  cargo build --release -p warp --bin warp-oss --features gui,creating_shared_sessions
  ```
- Typing `/remote-control` at the **zsh prompt** just yields
  `zsh: no such file or directory` — it's a Warp slash command, enter it in the
  agent input, not the terminal.

### Joining: use the deeplink, NOT the `app.warp.dev` link

The share UI shows `https://app.warp.dev/session/<id>`. That is Warp's **hosted
web viewer** and talks to Warp's cloud — it will **not** reach your relay (the
session exists only on your server, and the web app can't be repointed by an env
var). To join *your* relay, launch a second client with the **native deeplink**
and the same override:

```bash
WARP_SESSION_SHARING_SERVER_URL=ws://127.0.0.1:8787 \
  ./target/release/warp-oss "warposs://shared_session/<SESSION_ID>"
```

- Scheme is `warposs` (the OSS channel's URL scheme); the client rejects any
  other scheme for security, so an `https://…` arg is silently ignored.
- Translate mechanically: `app.warp.dev/session/<id>` → `warposs://shared_session/<id>`.
- Get `<SESSION_ID>` from the share link or the server's `session created` log.

### Autosuggestions look missing

Viewer inline suggestions come from a **local** history model that fills up only
as commands *complete during the shared session* — it is not seeded from the
sharer's existing shell history. A fresh viewer shows no suggestion until some
commands have run in-session. Rich AI autosuggestions need Warp's authenticated
prediction backend, which a relay-only/logged-out setup bypasses. Neither is a
relay defect.

### Interactive TUIs feel laggy

Block commands (`ls`, `git status`) are snappy. Typing into a full-screen
program (`vim`, `claude`, `top`) is keystroke → relay → sharer → PTY → back per
character, so it feels laggy — inherent to remote control, not the server (which
sits near 0% CPU). Judge responsiveness with block commands. Also: each GUI
instance is ~500 MB, so don't leave stale sharer/viewer windows running.

### Restart drops sessions

State is in-memory. Restarting `warp-server` invalidates all live session IDs;
clients must re-share. The old `session created` IDs won't be joinable.

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

Drive a running server with `N` concurrent sessions — each opens 1 sharer + 2
viewers, streams live output, and issues a remote command — to see multi-session
and remote control end to end:

```bash
# 1. start the server
cargo run -- --addr 127.0.0.1:8787 &

# 2. run the demo against it: <ws-url> <num-sessions>
cargo run --example multi_session_demo -- ws://127.0.0.1:8787 25
# => "25/25 sessions succeeded (no quota / limit errors)"
```

`N` defaults to 3 if omitted. Verified up to 25 concurrent sessions (75 live
connections) with no quota/limit errors.

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

## Security model

This relay is **unauthenticated by design** — there are no user accounts and no
quotas. Understand the trust model before exposing it:

- **A session's UUID is its only secret, and it grants full control.** Anyone who
  can reach the port *and* knows a live `session_id` can join that session and
  **remote-control the sharer's terminal** (run commands, write to the PTY). The
  Warp client connects to `/sessions/join/{id}` with **no password**, so the
  server cannot require one without breaking the real client — the 122-bit random
  UUIDv4 *is* the bearer token. Treat session links like secrets.
- **Keep the default loopback bind.** `--addr` defaults to `127.0.0.1:8787`. Only
  bind `0.0.0.0`/a LAN address if you trust everyone who can reach it.
- **No TLS in-process.** Traffic (including the session UUID, secret, and every
  keystroke) is plaintext `ws://`. For any non-loopback exposure, terminate TLS
  in a reverse proxy (caddy/nginx) and front the relay with `wss://`.
- Identity is placeholder (real Warp backend tokens are ignored) — avatars and
  display names are generic; view + control work regardless.

### Hardening that *is* enforced

- **Sharer resume requires the reconnect token.** `/sessions/{id}/resume` only
  hands over the sharer role to a client presenting the `reconnect_token` issued
  at creation — you can't hijack a session's sharer side by knowing just the id.
- **Sessions are reaped.** If a sharer's socket drops without `EndSession`, the
  session is held for `--session-grace-secs` (default 120) to allow a resume,
  then removed (viewers notified). No permanently-leaked sessions.
- **Bounded memory.** The terminal event log is a ring buffer
  (`MAX_EVENT_LOG`); per-connection writer queues are bounded
  (`WRITER_CHANNEL_CAP`) and a stalled peer is dropped (it can reconnect and
  catch up); inbound WS messages are capped at `--max-message-bytes` (16 MiB).

## Notes / limitations

- Opaque payloads (`AgentResponseEvent`, CRDT input ops) are relayed as bytes,
  never interpreted.
- A reconnecting viewer that fell further behind than `MAX_EVENT_LOG` events
  misses the trimmed ones; the screen self-heals on subsequent output.
- Not built for HA: one process, in-memory. For multi-machine scale you'd add a
  shared pub/sub layer (out of scope by design — latency-first).

## License

AGPL-3.0-only. This binary links the `session-sharing-protocol` crate, which is
AGPL-3.0; distributing `warp-server` therefore carries the same license. See
[`LICENSE`](LICENSE).
