#!/usr/local/bin/python3
"""Stands in for the orchestrator's `claude -p` (stream-json). Behaviour is keyed off words in the prompt."""
import json
import os
import subprocess
import sys
import time

args = sys.argv[1:]
prompt = args[args.index("-p") + 1]
resume = args[args.index("--resume") + 1] if "--resume" in args else None
home = os.environ["HOME"]
with open(os.path.join(home, "orch_calls.log"), "a") as f:
    f.write(json.dumps({"prompt": prompt, "resume": resume, "uid": os.getuid(), "run_id": os.environ.get("RUN_ID"),
                        "has_sprites_token": "SPRITES_TOKEN" in os.environ, "has_wa_token": "WA_AGENT_TOKEN" in os.environ,
                        "has_byteful": "BYTEFUL_PRIVATE_KEY" in os.environ}) + "\n")


def run(*cmd):
    return subprocess.run(cmd, check=True, capture_output=True, text=True).stdout


if prompt.startswith("[WhatsApp"):
    if "DELEGATE" in prompt:
        brief = "Make a report. CANCELME" if "CANCELME" in prompt else "Make a report."
        for _ in range(2 if "TWICE" in prompt else 1):
            run("delegate", brief)
            time.sleep(1.1)  # job ids are per-second
    if "RESIDENTIAL" in prompt:
        run("delegate", "--residential", "Residential job.")
    if "STATUS" in prompt:
        with open(os.path.join(home, "status_seen.txt"), "w") as f:
            f.write(run("delegate", "--status"))
if "NOTIFY" in prompt:
    run("notify", "progress update from the agent")
    shot = os.path.join(home, "shot.png")
    open(shot, "wb").write(b"\x89PNG fake")
    run("notify", "--file", shot, "a screenshot")
if "SLEEP" in prompt:
    time.sleep(60)

session = resume or f"sess-{int(time.time() * 1000)}"
print(json.dumps({"type": "system", "subtype": "init", "session_id": session}))
print(json.dumps({"type": "system", "subtype": "thinking_tokens", "estimated_tokens": 5, "session_id": session}))
print(json.dumps({"type": "assistant", "session_id": session,
                  "message": {"content": [{"type": "tool_use", "name": "Bash", "input": {"command": "echo checking"}}]}}))
flag = os.path.join(home, "flaky_failed_once")
if "FLAKY" in prompt and not os.path.exists(flag):
    open(flag, "w").close()
    print(json.dumps({"type": "result", "subtype": "error_during_execution", "is_error": True,
                      "result": "API Error: 503 upstream overloaded", "session_id": session}))
    sys.exit(1)
if "quiet" in prompt:
    result = "SILENT"
elif "MARKDOWN" in prompt:
    result = "## Done\n**bold** and [the link](https://example.com)"
elif "RETRYME" in prompt:
    result = "RETRYME reply"
else:
    result = "echo: " + prompt.strip().splitlines()[-1]
print(json.dumps({"type": "result", "subtype": "success", "is_error": False, "result": result, "session_id": session,
                  "num_turns": 2, "usage": {"input_tokens": 100, "cache_read_input_tokens": 900, "output_tokens": 50}}))
