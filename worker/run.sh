#!/bin/bash
# Runs one delegated job in the worker sprite: keeps the sprite awake, provisions it once,
# runs Claude on BRIEF.md, and records how it ended. Started detached by the bridge:
#   setsid -f nohup bash run.sh /workspace/agent/jobs/<id>
set -u
JOB="$1"
ROOT="$(dirname "$(dirname "$JOB")")"
ID="$(basename "$JOB")"
cd "$JOB"
put() { printf %s "$2" > "$1.tmp.$$" && mv -f "$1.tmp.$$" "$1"; }
put launcher.pid $$
mkdir -p output
find "$ROOT/jobs" -mindepth 1 -maxdepth 1 -type d -mtime +30 -exec rm -rf {} + 2> /dev/null  # keep a month of jobs
. "$ROOT/.env"
rm -f "$ROOT/.env"  # secrets live only in this process tree

# Hold the sprite awake while the job runs; a dead launcher lets it sleep within 5 minutes.
task="job-$ID"
(
  while [ ! -f exit_code ]; do
    sleep 60
    sprite-env curl -X PUT "/v1/tasks/$task" -d '{"expire":"5m"}' > /dev/null 2>&1
  done
) &
heartbeat=$!

# One-time setup; re-runs whenever provision.sh changes.
marker="$ROOT/.provisioned-$(sha256sum "$ROOT/provision.sh" | cut -c1-12)"
if [ ! -f "$marker" ] && [ ! -f cancelled ]; then
  bash "$ROOT/provision.sh" > provision.log 2>&1 && touch "$marker"
fi

pgrep -x Xvfb > /dev/null || setsid -f Xvfb :99 -screen 0 1920x1080x24 -nolisten tcp -ac > /dev/null 2>&1
export DISPLAY=:99

agent_exit=143
if [ ! -f "$marker" ]; then
  agent_exit=97  # provisioning failed: see provision.log
elif [ ! -f cancelled ]; then
  # Claude leads its own process group, so stopping it also stops the browser it started.
  setsid "$HOME/.local/bin/claude" -p "$(cat BRIEF.md)" \
    --permission-mode bypassPermissions --mcp-config "$ROOT/mcp.json" \
    --output-format stream-json --verbose \
    > agent.jsonl 2> agent.err &
  agent=$!
  deadline=$((SECONDS + ${WORKER_TIMEOUT_SECONDS:-3600}))
  (
    until [ -f cancelled ]; do
      kill -0 $agent 2> /dev/null || exit 0
      if [ $SECONDS -ge $deadline ]; then
        put timed_out ""
        break
      fi
      sleep 1
    done
    kill -TERM -- -$agent 2> /dev/null
    sleep 15
    kill -KILL -- -$agent 2> /dev/null
  ) &
  watcher=$!
  wait $agent
  agent_exit=$?
  kill $watcher 2> /dev/null
  kill -KILL -- -$agent 2> /dev/null
fi
put exit_code "$agent_exit"

kill $heartbeat 2> /dev/null
sprite-env curl -X DELETE "/v1/tasks/$task" > /dev/null 2>&1
