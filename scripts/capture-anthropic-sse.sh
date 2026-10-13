#!/usr/bin/env bash
# Capture ONE real Anthropic Messages SSE stream (text + reasoning + one tool
# call) as a deterministic fixture for the normalize-path tests.
#
# Uses the Claude-subscription OAuth shape (reference: ~/Work/Projects/CortexKit/
# anthropic-auth/packages/core/src/{constants,claude-code}.ts): Bearer access
# token + the OAuth/interleaved-thinking betas + a system prompt that LEADS with
# the Claude Code identity (OAuth rejects requests without it).
#
# The token is read directly from disk and never printed. The response carries
# no secret, but we scrub defensively before it is committed.
set -euo pipefail

AUTH_JSON="${AUTH_JSON:-$HOME/.local/share/opencode/auth.json}"
OUT="${1:-crates/broca-provider/tests/fixtures/anthropic-tool-call.sse}"
MODEL="${MODEL:-claude-opus-4-8}"

mkdir -p "$(dirname "$OUT")"

# Extract the OAuth access token + expiry without echoing the token.
read -r TOKEN EXPIRES < <(python3 - "$AUTH_JSON" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    data = json.load(f)
a = data.get("anthropic", {})
tok = a.get("access", "")
exp = a.get("expires", 0)
print(tok, exp)
PY
)

if [ -z "$TOKEN" ]; then
  echo "no anthropic access token in $AUTH_JSON" >&2
  exit 1
fi
NOW_MS=$(python3 -c 'import time; print(int(time.time()*1000))')
if [ "$EXPIRES" != "0" ] && [ "$NOW_MS" -ge "$EXPIRES" ]; then
  echo "anthropic access token expired (expires=$EXPIRES now=$NOW_MS) — refresh is Phase 3" >&2
  exit 2
fi

BODY=$(cat <<JSON
{
  "model": "$MODEL",
  "max_tokens": 2048,
  "stream": true,
  "thinking": { "type": "enabled", "budget_tokens": 1024 },
  "system": [
    { "type": "text", "text": "You are Claude Code, Anthropic's official CLI for Claude." }
  ],
  "messages": [
    { "role": "user", "content": "What time is it in Madrid? Think briefly about how you'd find out, then call the get_time tool." }
  ],
  "tools": [
    {
      "name": "get_time",
      "description": "Get the current time in a given timezone.",
      "input_schema": {
        "type": "object",
        "properties": { "timezone": { "type": "string", "description": "IANA timezone, e.g. Europe/Madrid" } },
        "required": ["timezone"]
      }
    }
  ],
  "tool_choice": { "type": "auto" }
}
JSON
)

echo "POST /v1/messages model=$MODEL → $OUT" >&2
curl -sS --no-buffer https://api.anthropic.com/v1/messages \
  -H "authorization: Bearer $TOKEN" \
  -H "anthropic-version: 2023-06-01" \
  -H "anthropic-beta: oauth-2025-04-20,interleaved-thinking-2025-05-14" \
  -H "content-type: application/json" \
  -H "accept: application/json" \
  -H "user-agent: claude-cli/2.1.177 (external, cli)" \
  -H "x-app: cli" \
  --data "$BODY" \
  -o "$OUT"

# Defensive scrub: fail loudly if any secret-looking material landed in the fixture.
if grep -qE 'sk-ant-|Bearer |"access"|"refresh"' "$OUT"; then
  echo "SECRET-LIKE CONTENT in $OUT — refusing to keep it" >&2
  rm -f "$OUT"
  exit 3
fi

echo "captured $(wc -c < "$OUT") bytes" >&2
echo "event types:" >&2
grep -oE '"type":"[a-z_]+"' "$OUT" | sort | uniq -c | sed 's/^/  /' >&2
