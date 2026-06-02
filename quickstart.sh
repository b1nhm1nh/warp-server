#!/usr/bin/env bash
#
# quickstart.sh — build & launch the warp-server relay, and optionally the
# Warp client pointed at it. One command to go from clone to a working,
# limit-free remote-control session.
#
# Usage:
#   ./quickstart.sh                      # build + run the relay (foreground)
#   ./quickstart.sh --client             # relay (background) + launch a sharer client
#   ./quickstart.sh --join <SESSION_ID>  # relay (background) + launch a viewer client
#
# Options:
#   --addr <host:port>   Bind address for the relay (default: 127.0.0.1:8787)
#   --warp <path>        Path to a warpdotdev/warp checkout (for --client/--join).
#                        Default: ../warp relative to this repo.
#   --client             After starting the relay, build & launch a sharer client.
#   --join <SESSION_ID>  After starting the relay, launch a viewer client that
#                        joins <SESSION_ID> via the warposs:// deeplink.
#   --no-build           Skip cargo build (use existing binaries).
#   -h, --help           Show this help.
#
# Notes:
#   * The client is only auto-launched with --client/--join. Plain invocation
#     just runs the relay so you can point your own client at it.
#   * The relay holds state in memory; stopping it invalidates session IDs.
#   * See the "Remote control: gotchas" section of README.md — in particular,
#     /remote-control needs the `creating_shared_sessions` feature (this script
#     builds the client with it) and joining must use the warposs:// deeplink
#     (this script does that for --join), NOT the app.warp.dev web link.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_ROOT"

# --- defaults ---
ADDR="127.0.0.1:8787"
WARP_DIR="$(cd "$REPO_ROOT/.." && pwd)/warp"
MODE="relay"            # relay | client | join
JOIN_ID=""
DO_BUILD=1

# --- arg parsing ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    --addr)      ADDR="$2"; shift 2 ;;
    --warp)      WARP_DIR="$2"; shift 2 ;;
    --client)    MODE="client"; shift ;;
    --join)      MODE="join"; JOIN_ID="${2:-}"; shift; [[ $# -gt 0 ]] && shift ;;
    --no-build)  DO_BUILD=0; shift ;;
    -h|--help)
      sed -n '2,40p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) echo "Unknown option: $1" >&2; exit 2 ;;
  esac
done

if [[ "$MODE" == "join" && -z "$JOIN_ID" ]]; then
  echo "error: --join requires a <SESSION_ID>" >&2
  exit 2
fi

WS_URL="ws://${ADDR}"
SERVER_BIN="$REPO_ROOT/target/release/warp-server"
CLIENT_BIN="$WARP_DIR/target/release/warp-oss"
SERVER_PID=""

cleanup() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    echo
    echo "==> stopping relay (pid $SERVER_PID)"
    kill "$SERVER_PID" 2>/dev/null || true
  fi
}

# --- build + start the relay ---
if [[ "$DO_BUILD" -eq 1 ]]; then
  echo "==> building warp-server (release)"
  cargo build --release --quiet
fi

# In plain relay mode, just exec the server in the foreground (Ctrl-C to stop).
if [[ "$MODE" == "relay" ]]; then
  echo "==> starting relay on $WS_URL (Ctrl-C to stop)"
  exec "$SERVER_BIN" --addr "$ADDR"
fi

# Otherwise we need the relay in the background so we can launch a client too.
trap cleanup EXIT INT TERM
echo "==> starting relay on $WS_URL (background)"
"$SERVER_BIN" --addr "$ADDR" &
SERVER_PID=$!

# Wait for /healthz.
HEALTH_URL="http://${ADDR}/healthz"
for _ in $(seq 1 100); do
  if curl -fsS "$HEALTH_URL" >/dev/null 2>&1; then break; fi
  sleep 0.1
done
if ! curl -fsS "$HEALTH_URL" >/dev/null 2>&1; then
  echo "error: relay did not become healthy at $HEALTH_URL" >&2
  exit 1
fi
echo "    relay healthy."

# --- client ---
if [[ ! -x "$CLIENT_BIN" ]]; then
  if [[ "$DO_BUILD" -eq 1 ]]; then
    if [[ ! -d "$WARP_DIR" ]]; then
      echo "error: warp checkout not found at '$WARP_DIR' (use --warp <path>)" >&2
      exit 1
    fi
    echo "==> building warp-oss client (with creating_shared_sessions)"
    ( cd "$WARP_DIR" && cargo build --release -p warp --bin warp-oss \
        --features gui,creating_shared_sessions )
  else
    echo "error: client binary not found at '$CLIENT_BIN' and --no-build set" >&2
    exit 1
  fi
fi

case "$MODE" in
  client)
    echo "==> launching SHARER client (pointed at $WS_URL)"
    echo "    In the agent input box, run: /remote-control"
    WARP_SESSION_SHARING_SERVER_URL="$WS_URL" "$CLIENT_BIN"
    ;;
  join)
    echo "==> launching VIEWER client to join $JOIN_ID (via $WS_URL)"
    WARP_SESSION_SHARING_SERVER_URL="$WS_URL" \
      "$CLIENT_BIN" "warposs://shared_session/${JOIN_ID}"
    ;;
esac
