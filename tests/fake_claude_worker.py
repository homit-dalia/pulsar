#!/usr/local/bin/python3
"""Stands in for the worker's `claude -p` inside the (fake) sprite, emitting stream-json like Claude Code."""
import json
import os
import sys
import time

args = sys.argv[1:]
brief = args[args.index("-p") + 1]
with open("/tmp/worker_calls.log", "a") as f:
    f.write(json.dumps({"brief": brief, "cwd": os.getcwd(), "display": os.environ.get("DISPLAY"),
                        "has_model_token": os.environ.get("ANTHROPIC_AUTH_TOKEN") == "deepseek-test",
                        "agent_var": os.environ.get("AGENT_EXAMPLE"), "mcp": "--mcp-config" in args,
                        "timeout": os.environ.get("WORKER_TIMEOUT_SECONDS"), "run_id": os.environ.get("RUN_ID"),
                        "has_byteful": os.environ.get("BYTEFUL_PRIVATE_KEY") == "priv-test"
                        and os.environ.get("BYTEFUL_PUBLIC_KEY") == "pub-test"}) + "\n")


def emit(event):
    print(json.dumps({"session_id": "worker-session", **event}), flush=True)


emit({"type": "system", "subtype": "init"})
emit({"type": "system", "subtype": "thinking_tokens", "estimated_tokens": 3})
emit({"type": "assistant", "message": {"content": [{"type": "text", "text": "Starting on the report"}]}})
emit({"type": "assistant", "message": {"content": [{"type": "tool_use", "name": "Bash", "input": {"command": "make report"}}]}})
time.sleep(120 if "CANCELME" in brief else 3)
os.makedirs("output", exist_ok=True)
with open("output/report.txt", "w") as f:
    f.write("the report\n")
emit({"type": "result", "subtype": "success", "is_error": False, "result": "Report done: wrote report.txt", "num_turns": 3,
      "usage": {"input_tokens": 10, "cache_read_input_tokens": 90, "output_tokens": 5}})
