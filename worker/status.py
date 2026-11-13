"""Summarizes a worker job for the bridge as one small JSON object (run inside the sprite).

    python3 status.py <job dir>        state, latest step, outputs: polled every 30s
    python3 status.py <job dir> full   the same plus the job's step log, read once at the end
"""
import json
import pathlib
import sys

job = pathlib.Path(sys.argv[1])
full = sys.argv[2:] == ["full"]
exit_file = job / "exit_code"
finished = exit_file.exists()
DETAIL = 2000 if full else 160


def clip(text, limit=DETAIL):
    text = " ".join(str(text).split())
    return text if len(text) <= limit else text[:limit] + "..."


def tail(path, size=2000):
    return path.read_text(errors="replace")[-size:] if path.exists() else ""


events = []
stream = job / "agent.jsonl"
if stream.exists():
    with stream.open("rb") as handle:
        start = 0 if full else max(handle.seek(0, 2) - 256 * 1024, 0)
        handle.seek(start)
        lines = handle.read().decode("utf-8", "replace").split("\n")
    for line in lines[1:] if start else lines:
        if '"thinking_tokens"' in line[:200]:
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            pass

steps = []
for event in events:
    kind = event.get("type")
    if kind == "assistant":
        for block in event.get("message", {}).get("content", []):
            if block.get("type") == "text" and block.get("text", "").strip():
                steps.append({"text": clip(block["text"])})
            elif block.get("type") == "tool_use":
                steps.append({"tool": block.get("name"), "input": clip(json.dumps(block.get("input")))})
    elif kind == "user":
        for block in event.get("message", {}).get("content", []) or []:
            if isinstance(block, dict) and block.get("is_error"):
                steps.append({"error": clip(block.get("content"))})

progress = None
for step in reversed(steps):
    if "text" in step or "tool" in step:
        progress = step.get("text") or f"{step['tool']}: {step.get('input', '')}"
        progress = progress[:160]
        break

result = next((e for e in reversed(events) if e.get("type") == "result"), None) if finished else None
usage = None
if result:
    usage = {k: v for k, v in (result.get("usage") or {}).items()
             if k in ("input_tokens", "cache_read_input_tokens", "output_tokens")}
    usage.update(num_turns=result.get("num_turns"), duration_ms=result.get("duration_ms"))
output = job / "output"
summary = {
    "state": "finished" if finished else "running",
    "exit_code": int(exit_file.read_text().strip() or 1) if finished else None,
    "final_message": (result or {}).get("result"),
    "progress": progress,
    "cancelled": (job / "cancelled").exists(),
    "timed_out": (job / "timed_out").exists(),
    "log_tail": tail(job / "agent.err") or tail(job / "provision.log"),
    "outputs": sorted(p.name for p in output.iterdir() if p.is_file()) if output.is_dir() else [],
    "usage": usage,
    "session_id": (result or {}).get("session_id"),
}
if full:
    summary["steps"] = steps[-300:]
print(json.dumps(summary))
