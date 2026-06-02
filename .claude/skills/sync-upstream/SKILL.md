---
name: sync-upstream
description: Sync warp-server with a newer upstream warpdotdev/warp — re-apply the OSS server-URL-override patch, match the session-sharing-protocol rev, and rebuild/retest to catch protocol drift. Use when pulling new upstream commits or when session sharing breaks after an upstream update.
---

# sync-upstream

Keep `warp-server` working against a moving `warpdotdev/warp`. There are only
two coupling points; everything else is self-contained.

1. **Client patch** — `patches/0001-allow-oss-session-sharing-url-override.patch`
   (one line in `crates/warp_core/src/channel/mod.rs` + a `mod_test.rs` test).
2. **Protocol rev** — `session-sharing-protocol` git dep, which MUST match the
   rev the Warp client uses or the JSON wire format drifts.

Full prose reference: `docs/SYNCING.md`. This skill is the executable procedure.

## Preconditions
- The upstream Warp checkout and this repo are reachable (commonly sibling dirs
  `warp/` and `warp-server/`). Ask the user for the warp checkout path if unknown.
- Don't push or commit unless the user asks.

## Procedure

### 1. Update upstream
```bash
cd <warp>
git fetch origin && git checkout origin/master   # or the target tag/branch
```

### 2. Re-apply the patch
```bash
git apply <warp-server>/patches/0001-allow-oss-session-sharing-url-override.patch
```
- Clean → continue.
- Conflict → the target moved. Edit `crates/warp_core/src/channel/mod.rs`: in
  `fn allows_server_url_overrides`, put `Channel::Oss` in the arm returning
  `true`. Keep the match exhaustive (no `_`). Then regenerate the patch (step 5).

Verify:
```bash
cargo test -p warp_core channel::    # server_url_override_allowed_per_channel passes
```

### 3. Match the protocol rev
```bash
grep session-sharing-protocol <warp>/Cargo.toml
grep session-sharing-protocol <warp-server>/Cargo.toml
```
If the `rev=` differs, set this repo's `Cargo.toml` to the client's rev:
```bash
cd <warp-server>
cargo update -p session-sharing-protocol
cargo build
cargo test                           # protocol_smoke + relay_e2e catch wire drift
cargo clippy --all-targets -- -D warnings
```

### 4. Resolve protocol drift (only if step 3 fails to build/test)
A new rev may add/rename message variants or fields, surfacing as
non-exhaustive-match or missing-field errors in `sharer_ws.rs` / `viewer_ws.rs`.
Read the new crate source and update the handlers:
```bash
git clone https://github.com/warpdotdev/session-sharing-protocol /tmp/ssp
cd /tmp/ssp && git checkout <new-rev>   # read src/{lib,sharer,viewer}.rs, src/common/*.rs
```
Map each new sharer→server / viewer→server variant to the right relay action
(broadcast to viewers, forward to sharer, or intentional no-op). Never add a `_`
wildcard — exhaustive matching is what makes the next drift a compile error.

### 5. Regenerate the patch (only if step 2 needed hand-edits)
```bash
cd <warp>
git add -N crates/warp_core/src/channel/mod_test.rs
git diff crates/warp_core/src/channel/mod.rs \
         crates/warp_core/src/channel/mod_test.rs \
  > <warp-server>/patches/0001-allow-oss-session-sharing-url-override.patch
git reset -q crates/warp_core/src/channel/mod_test.rs
git apply --reverse --check <warp-server>/patches/0001-...patch   # exit 0 = faithful
```

### 6. Verify end to end
```bash
cd <warp-server> && cargo run -- --addr 127.0.0.1:8787 &
cargo run --example multi_session_demo -- ws://127.0.0.1:8787 5
# expect: "5/5 sessions succeeded (no quota / limit errors)"
```
Optionally rebuild the client: `cargo build -p warp --bin warp-oss --features gui`.

## Report back
Summarize: patch applied cleanly or needed edits; old vs new protocol rev;
whether handlers changed; test + demo results. Flag any new protocol variant you
had to handle and how you mapped it.
