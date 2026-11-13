"""Summarizes a worker job for the bridge as one small JSON object (run inside the sprite)."""
import json
import pathlib
import sys

job = pathlib.Path(sys.argv[1])
exit_file = job / "exit_code"
finished = exit_file.exists()


def clip(text, limit=160):
    text = " ".join(str(text).split())
    return text if len(text) <= limit else text[:limit] + "..."


def tail(path, size=2000):
    return path.read_text(errors="replace")[-size:] if path.exists() else ""


events = []
stream = job / "agent.jsonl"
if stream.exists():
    with stream.open("rb") as handle:
        start = max(handle.seek(0, 2) - 256 * 1024, 0)
        handle.seek(start)
        lines = handle.read().decode("utf-8", "replace").split("\n")
    for line in lines[1:] if start else lines:
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            pass

progress = None
for event in reversed(events):
    if event.get("type") != "assistant":
        continue
    for block in reversed(event.get("message", {}).get("content", [])):
        if block.get("type") == "text" and block.get("text", "").strip():
            progress = clip(block["text"])
        elif block.get("type") == "tool_use":
            params = block.get("input") or {}
            detail = next((params[k] for k in ("command", "url", "file_path", "description") if params.get(k)), "")
            progress = clip(f"{block.get('name')}: {detail}")
        if progress:
            break
    if progress:
        break

result = next((e for e in reversed(events) if e.get("type") == "result"), None) if finished else None
output = job / "output"
print(json.dumps({
    "state": "finished" if finished else "running",
    "exit_code": int(exit_file.read_text().strip() or 1) if finished else None,
    "final_message": (result or {}).get("result"),
    "progress": progress,
    "cancelled": (job / "cancelled").exists(),
    "timed_out": (job / "timed_out").exists(),
    "log_tail": tail(job / "agent.err") or tail(job / "provision.log"),
    "outputs": sorted(p.name for p in output.iterdir() if p.is_file()) if output.is_dir() else [],
}))
