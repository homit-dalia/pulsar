#!/bin/bash
# Runs inside the orchestrator image as root. The container plays both the orchestrator machine and,
# through fake_sprites.py, the worker sprite. The real bridge.py, run.sh, status.py, delegate and
# notify run; WhatsApp, Sprites and both Claudes are fakes, and Postgres is real (a sibling container).
set -eu
cp -r /app /tmp/app
cat > /tmp/app/worker/provision.sh <<'EOF'
#!/bin/bash
# Fails once (like a flaky apt mirror), then works, so the bridge's retry is exercised.
if [ ! -f /tmp/prov-failed-once ]; then touch /tmp/prov-failed-once; echo "simulated apt failure"; exit 1; fi
echo "provision stub ran"
EOF
mkdir -p /sprite-home/.local/bin
install -m 755 /tests/fake_claude_worker.py /sprite-home/.local/bin/claude
install -m 755 /tests/fake_claude_orchestrator.py /usr/local/bin/orch-claude
printf '#!/bin/sh\necho "$*" >> /tmp/sprite-env.log\n' > /usr/local/bin/sprite-env && chmod 755 /usr/local/bin/sprite-env

python3 /tests/fake_whatsapp.py 18999 > /tmp/fake_whatsapp.log 2>&1 &
python3 /tests/fake_sprites.py 18998 > /tmp/fake_sprites.out 2>&1 &
sleep 1

export DATA_DIR=/data TZ=America/New_York \
  WA_API_BASE=http://127.0.0.1:18999/agent/v1 WA_AGENT_TOKEN=wa-test-token \
  SPRITES_API_URL=http://127.0.0.1:18998/v1 SPRITES_TOKEN=sprites-test-token SPRITE_NAME=agent-worker \
  CLAUDE_BIN=/usr/local/bin/orch-claude ANTHROPIC_AUTH_TOKEN=deepseek-test AGENT_EXAMPLE=visible \
  DATABASE_URL=postgresql://agent_rw:test-pw@host.docker.internal:55432/postgres \
  WORKER_TIMEOUT_MINUTES=1 RETRY_BASE_SECONDS=2 BYTEFUL_PUBLIC_KEY=pub-test BYTEFUL_PRIVATE_KEY=priv-test
exec /opt/bridge/bin/python /tmp/app/bridge.py
