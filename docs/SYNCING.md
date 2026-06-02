# Syncing `warp-server` with upstream Warp

`warp-server` couples to the Warp client in exactly two places:

1. **The client patch** — `patches/0001-allow-oss-session-sharing-url-override.patch`,
   a one-line change to `crates/warp_core/src/channel/mod.rs` (plus a unit test in
   `mod_test.rs`) that lets the OSS build honor `WARP_SESSION_SHARING_SERVER_URL`.
2. **The protocol crate rev** — both this server and the Warp client depend on
   `session-sharing-protocol` (a git dependency). They must be pinned to the
   **same rev** or the JSON wire format can drift and messages fail to parse.

Everything else (the relay logic) is self-contained. So "syncing with upstream"
is just: keep the patch applying, and keep the protocol rev matched.

## When to run this

- Upstream `warpdotdev/warp` has new commits you want to build against, **or**
- Session sharing misbehaves after an upstream pull (suspect a protocol bump).

## Checklist

Assume `warp/` (the upstream checkout) and `warp-server/` are sibling dirs.

### 1. Update the upstream checkout
```bash
cd warp
git fetch origin && git checkout origin/master   # or the tag/branch you target
```

### 2. Re-apply the client patch
```bash
git apply ../warp-server/patches/0001-allow-oss-session-sharing-url-override.patch
```
- **Clean apply** → done with this step.
- **Conflict / "patch does not apply"** → the target moved. Open
  `crates/warp_core/src/channel/mod.rs`, find `fn allows_server_url_overrides`,
  and ensure `Channel::Oss` is in the arm that returns `true`. Then regenerate
  the patch (see [Regenerating the patch](#regenerating-the-patch)).

Verify:
```bash
cargo test -p warp_core channel::    # server_url_override_allowed_per_channel must pass
```

### 3. Match the protocol rev
Read the rev the client now uses and compare to ours:
```bash
grep 'session-sharing-protocol' warp/Cargo.toml
grep 'session-sharing-protocol' warp-server/Cargo.toml
```
If they differ, update `warp-server/Cargo.toml` to the client's `rev`, then:
```bash
cd warp-server
cargo update -p session-sharing-protocol
cargo build
cargo test                           # protocol_smoke + relay_e2e catch wire drift
cargo clippy --all-targets -- -D warnings
```

### 4. If the protocol changed shape
A new rev may add/rename message variants or fields. Symptoms: build errors in
`sharer_ws.rs` / `viewer_ws.rs` (non-exhaustive match, missing field), or
`relay_e2e` failures. Fix by reading the updated crate source:
```bash
# the crate is checked out under cargo's git cache, or clone it directly:
git clone https://github.com/warpdotdev/session-sharing-protocol /tmp/ssp
cd /tmp/ssp && git checkout <new-rev>
# read src/{lib,sharer,viewer}.rs and src/common/*.rs, update handlers to match.
```
Our `match` arms are deliberately exhaustive (no `_` wildcard) so a new upstream
variant surfaces as a compile error rather than silently dropping a message.

### 5. Done
Commit any `Cargo.toml` / handler / patch changes in `warp-server`. Rebuild the
client (`cargo build -p warp --bin warp-oss --features gui`) and smoke-test with
the live demo:
```bash
warp-server --addr 127.0.0.1:8787 &
cargo run --example multi_session_demo -- ws://127.0.0.1:8787 5
```

## Regenerating the patch

After hand-editing `mod.rs` (+ `mod_test.rs`) in the upstream checkout:
```bash
cd warp
git add -N crates/warp_core/src/channel/mod_test.rs
git diff crates/warp_core/src/channel/mod.rs \
         crates/warp_core/src/channel/mod_test.rs \
  > ../warp-server/patches/0001-allow-oss-session-sharing-url-override.patch
git reset -q crates/warp_core/src/channel/mod_test.rs
```
Sanity check it reproduces the working-tree change exactly:
```bash
git apply --reverse --check ../warp-server/patches/0001-allow-oss-session-sharing-url-override.patch
```
(exit 0 = the patch matches the current edit).
