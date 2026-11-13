#!/usr/bin/env python3
"""Plumbing for a personal agent: WhatsApp <-> an orchestrator Claude, plus one worker Sprite.

A single process on one small, always-on Fly Machine:
  * long-polls the WhatsApp Agent Platform and queues the owner's messages
  * fires due rows of agent.schedules (Postgres)
  * runs the orchestrator (`claude -p`), one job at a time: the owner's messages first, then
    worker reports, then scheduled tasks; quick follow-up messages are merged into one turn
  * runs the jobs the orchestrator `delegate`s on one Fly Sprite, which has its own Claude and a
    browser and sleeps when idle. They queue in agent.runs, and each report comes back to the
    orchestrator as a new job
  * records every run (with a step log) in agent.runs and every message in agent.messages.
    Outbound messages are only ever queued there, and one sender delivers them in order

Transient failures are retried: model/API errors in a turn (resuming the same session),
worker starts and provisioning, and WhatsApp sends. No agent logic lives here: the agents'
instructions are agent/CLAUDE.md and worker/CLAUDE.md.

WhatsApp Agent Platform: https://www.whatsapp.com/developer/WhatsApp-Agent-Platform-Developer-Manual.pdf
Sprites API: https://docs.sprites.dev
"""
from __future__ import annotations

import hashlib
import json
import mimetypes
import os
import pwd
import re
import shlex
import shutil
import signal
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

WA_API = os.environ.get("WA_API_BASE", "https://api.whatsapp.com/agent/v1")
WA_TOKEN = os.environ.get("WA_AGENT_TOKEN", "")
SPRITES_API = os.environ.get("SPRITES_API_URL", "https://api.sprites.dev/v1").rstrip("/")
SPRITES_TOKEN = os.environ.get("SPRITES_TOKEN", "")
SPRITE = os.environ.get("SPRITE_NAME", "agent-worker")
SPRITE_ROOT = "/workspace/agent"
DATA = Path(os.environ.get("DATA_DIR", "/data"))
APP = Path(__file__).resolve().parent
CLAUDE_BIN = os.environ.get("CLAUDE_BIN", "claude")
AGENT_USER = "agent"
JOB_TIMEOUT = float(os.environ.get("JOB_TIMEOUT_MINUTES", "30")) * 60
WORKER_TIMEOUT = float(os.environ.get("WORKER_TIMEOUT_MINUTES", "60")) * 60
CHAT_IDLE_SECONDS = float(os.environ.get("CHAT_SESSION_IDLE_HOURS", "6")) * 3600
MAX_TURNS = os.environ.get("MAX_TURNS", "100")
RETRY_SECONDS = float(os.environ.get("RETRY_BASE_SECONDS", "30"))  # base for every backoff below
TURN_ATTEMPTS, WORKER_ATTEMPTS, SEND_ATTEMPTS = 3, 3, 6

STOP = threading.Event()
TABLES_READY = threading.Event()
RUN = {"proc": None, "stopped": False, "timed_out": False, "job": None}
SESSION_FILE = DATA / "state" / "chat_session.json"
USER_FILE = DATA / "state" / "wa_user"
PRIORITY = {"messages": 0, "report": 1, "task": 2}

# An error worth retrying, unless it's one of the permanent ones (bad key, no balance).
TRANSIENT = re.compile(r"overload|rate.?limit|\b429\b|\b5\d\d\b|time[d ]?out|temporar|unavailable|connection|"
                       r"network|econn|socket|reset by peer|api error|internal server error", re.I)
PERMANENT = re.compile(r"insufficient|balance|\b40[123]\b|invalid.{0,20}(key|token)|authenticat|unauthori", re.I)


def log(*parts):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), *parts, flush=True)


def now_str():
    return datetime.now().astimezone().strftime("%a %Y-%m-%d %H:%M %Z")


def clip(text, limit):
    text = str(text)
    return text if len(text) <= limit else text[:limit] + "…"


def transient(error):
    return bool(TRANSIENT.search(error or "")) and not PERMANENT.search(error or "")


def http(method, url, body=None, headers=None, timeout=60):
    """Returns (status, body bytes). Network failures raise OSError."""
    headers = dict(headers or {})
    if isinstance(body, (dict, list)):
        body = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def owner():
    return USER_FILE.read_text().strip() if USER_FILE.exists() else ""


# ---------------------------------------------------------------- Postgres

_db = None
DB_LOCK = threading.Lock()


def sql(query, *args):
    """Runs one query (or, without args, a whole script) on a shared autocommit connection."""
    global _db
    import psycopg

    with DB_LOCK:
        for attempt in range(2):
            try:
                if _db is None or _db.closed:
                    _db = psycopg.connect(
                        os.environ["DATABASE_URL"], autocommit=True, prepare_threshold=None, connect_timeout=15
                    )
                cursor = _db.execute(query, args) if args else _db.execute(query)
                return cursor.fetchall() if cursor.description else []
            except psycopg.OperationalError:
                _db = None
                if attempt:
                    raise
                time.sleep(2)


def get_state(key):
    rows = sql("select value from agent.bridge_state where key = %s", key)
    return rows[0][0] if rows else None


def set_state(key, value):
    sql(
        "insert into agent.bridge_state (key, value) values (%s, %s) "
        "on conflict (key) do update set value = excluded.value, updated_at = now()",
        key, str(value),
    )


def jsonb(value):
    return None if value is None else json.dumps(value)


def start_run(kind, label, text, meta=None):
    """Records the start of an orchestrator turn; returns its id (None if Postgres is down)."""
    try:
        return sql("insert into agent.runs (kind, label, input, meta) values (%s, %s, %s, %s::jsonb) returning id",
                   kind, label, text[:50000], jsonb(meta or {}))[0][0]
    except Exception as e:  # noqa: BLE001 - telemetry must never block the work
        log("run not recorded:", repr(e))
        return None


def finish_run(run_id, status, output="", session=None, steps=None, usage=None, attempts=None, meta=None):
    if run_id is None:
        return
    try:
        sql("update agent.runs set status = %s, output = %s, session_id = coalesce(%s, session_id), "
            "steps = coalesce(%s::jsonb, steps), usage = coalesce(%s::jsonb, usage), "
            "attempts = coalesce(%s, attempts), meta = meta || coalesce(%s::jsonb, '{}'), finished_at = now() "
            "where id = %s",
            status, (output or "")[:50000], session, jsonb(steps), jsonb(usage), attempts, jsonb(meta), run_id)
    except Exception as e:  # noqa: BLE001
        log("run not updated:", repr(e))


# ---------------------------------------------------------------- WhatsApp

SEND_LOCK = threading.Lock()  # the API asks for no concurrent sends to one recipient


def wa(method, path, body=None, headers=None, timeout=60):
    headers = {**(headers or {}), "Authorization": f"Bearer {WA_TOKEN}"}
    return http(method, WA_API + path, body, headers, timeout)


def whatsapp_format(text):
    """Turns the Markdown models like to write into WhatsApp's own formatting."""
    text = re.sub(r"\*\*(.+?)\*\*", r"*\1*", text)
    text = re.sub(r"~~(.+?)~~", r"~\1~", text)
    text = re.sub(r"(?m)^#{1,6}\s+(.+?)\s*#*\s*$", r"*\1*", text)
    return re.sub(r"\[([^\]]+)\]\((https?://[^)\s]+)\)", r"\1 (\2)", text)


def _send(payload):
    """One POST /messages. Returns (message id, error, retryable)."""
    try:
        status, response = wa("POST", "/messages", payload, timeout=120)
    except OSError as e:  # outcome unknown: a retry may duplicate, which beats losing it
        return None, f"network error: {e}", True
    if status == 200:
        return json.loads(response)["messages"][0]["id"], None, False
    return None, f"HTTP {status}: {response[:300]!r}", status == 429 or status >= 500


def send_text(user, text):
    text = whatsapp_format(text)
    result = (None, "empty message", False)
    with SEND_LOCK:
        for i in range(0, len(text), 4000):
            payload = {"messaging_product": "whatsapp", "to": user, "type": "text", "text": {"body": text[i : i + 4000]}}
            result = _send(payload)
            if not result[0]:
                break
    return result


def send_file(user, path, caption=""):
    path = Path(path)
    name = path.name.replace('"', "")
    mime = mimetypes.guess_type(name)[0] or "application/octet-stream"
    kind = {"image/jpeg": "image", "image/png": "image", "video/mp4": "video"}.get(mime)
    kind = kind or ("audio" if mime.startswith("audio/") else "document")
    boundary = uuid.uuid4().hex
    body = (
        f'--{boundary}\r\nContent-Disposition: form-data; name="messaging_product"\r\n\r\nwhatsapp\r\n'
        f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{name}"\r\n'
        f"Content-Type: {mime}\r\n\r\n"
    ).encode() + path.read_bytes() + f"\r\n--{boundary}--\r\n".encode()
    content_type = {"Content-Type": f"multipart/form-data; boundary={boundary}"}
    try:
        status, response = wa("POST", "/media", body, content_type, timeout=300)
    except OSError as e:
        return None, f"upload network error: {e}", True
    if status != 200:
        return None, f"upload failed with HTTP {status}: {response[:300]!r}", status == 429 or status >= 500
    media = {"id": json.loads(response)["id"]}
    if caption and kind != "audio":
        media["caption"] = whatsapp_format(caption)[:1024]
    if kind == "document":
        media["filename"] = name
    with SEND_LOCK:
        return _send({"messaging_product": "whatsapp", "to": user, "type": kind, kind: media})


def deliver_now(user, body, file_path=None):
    """Sends one message to the owner; returns (message id, error, retryable)."""
    if not user:
        return None, "the owner hasn't messaged yet", True
    if not file_path:
        return send_text(user, body)
    try:
        st = os.stat(file_path)
    except OSError as e:
        return None, str(e), False
    agent_uid = agent_ids()[0] if os.geteuid() == 0 else os.geteuid()
    if st.st_uid != agent_uid and not st.st_mode & 0o004:  # only files the agent could read itself
        return None, f"{file_path} isn't readable by the agent", False
    return send_file(user, file_path, body)


def queue_message(text="", file_path=None, run_id=None):
    """Everything the owner is sent goes through agent.messages; sender_loop delivers it."""
    text = (text or "").strip()
    if not text and not file_path:
        return
    try:
        sql("insert into agent.messages (direction, body, file_path, run_id) values ('out', %s, %s, %s)",
            text, file_path, run_id)
    except Exception as e:  # noqa: BLE001 - Postgres is down: send it directly rather than lose it
        log("couldn't queue a message, sending directly:", repr(e))
        deliver_now(owner(), text, file_path)


def sender_loop():
    """Delivers queued outbound messages strictly in order, about once a second. A message that
    fails transiently is retried with backoff, and nothing behind it overtakes it."""
    TABLES_READY.wait()
    while not STOP.is_set():
        try:
            user = owner()
            rows = sql("select id, body, file_path, attempts, next_attempt_at <= now() or next_attempt_at is null "
                       "from agent.messages where direction = 'out' and status = 'pending' order by id limit 20") if user else []
            for mid, body, file_path, attempts, due in rows:
                if not due:
                    break
                wamid, error, retryable = deliver_now(user, body, file_path)
                if wamid:
                    sql("update agent.messages set status = 'sent', wa_message_id = %s, error = null, sent_at = now(), "
                        "attempts = attempts + 1 where id = %s", wamid, mid)
                elif retryable and attempts + 1 < SEND_ATTEMPTS:
                    delay = RETRY_SECONDS / 2 * 2**attempts
                    sql("update agent.messages set attempts = attempts + 1, error = %s, "
                        "next_attempt_at = now() + make_interval(secs => %s) where id = %s", error, delay, mid)
                    break
                else:
                    sql("update agent.messages set status = 'failed', error = %s, attempts = attempts + 1 where id = %s",
                        error, mid)
            time.sleep(1)
        except Exception as e:  # noqa: BLE001
            log("sender:", repr(e))
            time.sleep(5)


def record_inbound(messages):
    for msg in messages:
        kind = msg.get("type")
        if kind == "text":
            body = msg["text"]["body"]
        elif kind == "reaction":
            body = f"[reaction {msg['reaction'].get('emoji') or '(removed)'}]"
        else:
            media = msg.get(kind) or {}
            body = " ".join(filter(None, [f"[{kind}]", media.get("filename"), media.get("caption")]))
        sql("insert into agent.messages (direction, body, status, wa_message_id) values ('in', %s, 'received', %s) "
            "on conflict (wa_message_id) do nothing", body, msg["id"])


def download(media):
    status, response = wa("GET", f"/media/{media['id']}")
    if status != 200:
        raise RuntimeError(f"HTTP {status}")
    meta = json.loads(response)
    ext = mimetypes.guess_extension((meta.get("mime_type") or "").split(";")[0]) or ""
    name = Path(media.get("filename") or f"{media['id']}{ext}").name
    status, data = http("GET", meta["url"], headers={"Authorization": f"Bearer {WA_TOKEN}"}, timeout=300)
    if status != 200:
        raise RuntimeError(f"HTTP {status}")
    path = DATA / "inbox" / f"{time.strftime('%Y%m%d-%H%M%S')}-{name}"
    path.write_bytes(data)
    give_to_agent(path)
    return path


def describe(msg):
    """One prompt line per inbound message; attachments are saved to /data/inbox."""
    kind = msg.get("type")
    quoted = msg.get("context", {}).get("id")
    quoted = f" (replying to your message {quoted})" if quoted else ""
    if kind == "text":
        return msg["text"]["body"] + quoted
    if kind == "reaction":
        reaction = msg["reaction"]
        return f"[reacted {reaction.get('emoji') or '(removed)'} to message {reaction.get('message_id')}]"
    media = msg.get(kind) or {}
    try:
        path = download(media)
    except Exception as e:  # noqa: BLE001 - report it to the agent and carry on
        return f"[{kind} attachment could not be downloaded: {e}]"
    label = "voice note" if media.get("voice") else kind
    caption = f' with caption "{media["caption"]}"' if media.get("caption") else ""
    return f"[{label} saved to {path}{caption}]{quoted}"


def keep_typing(wamid):
    """Refreshes the typing indicator (it lapses after 25s) until the returned event is set."""
    done = threading.Event()

    def loop():
        while not done.wait(20):
            try:
                wa("POST", "/statuses", {"messaging_product": "whatsapp", "status": "read", "message_id": wamid,
                                         "typing_indicator": {"type": "text"}})
            except OSError:
                pass

    threading.Thread(target=loop, daemon=True).start()
    return done


# ---------------------------------------------------------------- orchestrator turns


def agent_ids():
    entry = pwd.getpwnam(AGENT_USER)
    return entry.pw_uid, entry.pw_gid


def as_agent():
    """Popen kwargs that drop root to the agent user (Claude Code won't bypass permissions as root)."""
    return {"user": AGENT_USER, "group": AGENT_USER, "extra_groups": []} if os.geteuid() == 0 else {}


def give_to_agent(path):
    if os.geteuid() == 0:
        os.chown(path, *agent_ids())


def agent_env(run_id=None):
    """The orchestrator's environment: model settings, the database and AGENT_* credentials only."""
    keep = ("ANTHROPIC_", "CLAUDE_CODE_", "DISABLE_", "AGENT_")
    env = {k: v for k, v in os.environ.items() if k.startswith(keep)}
    env.update({k: os.environ[k] for k in ("API_TIMEOUT_MS", "DATABASE_URL", "TZ") if k in os.environ})
    home = DATA / "home"
    env.update(
        HOME=str(home), USER=AGENT_USER, LOGNAME=AGENT_USER, SHELL="/bin/bash", LANG="C.UTF-8",
        DATA_DIR=str(DATA), PATH=f"{home}/.local/bin:/usr/local/bin:/usr/bin:/bin", RUN_ID=str(run_id or ""),
    )
    return env


def kill_group(proc):
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(proc.pid, sig)
        except ProcessLookupError:
            return
        try:
            proc.wait(timeout=10)
            return
        except subprocess.TimeoutExpired:
            pass


def summarize_stream(lines, limit=300, detail=500):
    """Reads Claude Code's stream-json events: (steps, final result event, session id)."""
    steps, result, session = [], None, None
    for line in lines:
        try:
            event = json.loads(line)
        except ValueError:
            continue
        session = event.get("session_id") or session
        kind = event.get("type")
        if kind == "assistant":
            for block in event.get("message", {}).get("content", []):
                if block.get("type") == "text" and block.get("text", "").strip():
                    steps.append({"text": clip(block["text"].strip(), detail)})
                elif block.get("type") == "tool_use":
                    steps.append({"tool": block.get("name"), "input": clip(json.dumps(block.get("input")), detail)})
        elif kind == "user":
            for block in event.get("message", {}).get("content", []) or []:
                if isinstance(block, dict) and block.get("is_error"):
                    steps.append({"error": clip(block.get("content"), detail)})
        elif kind == "result":
            result = event
    return steps[-limit:], result, session


def run_claude(prompt, resume=None, run_id=None):
    """Runs one orchestrator turn. Returns (reply, session, status, steps, usage). The full event
    stream is kept in /data/runs/<run id>.jsonl so the agent can look back at exactly what it did."""
    transcript = DATA / "runs" / f"{run_id or 'norun-' + uuid.uuid4().hex[:8]}.jsonl"
    start = transcript.stat().st_size if transcript.exists() else 0
    cmd = [CLAUDE_BIN, "-p", prompt, "--output-format", "stream-json", "--verbose",
           "--permission-mode", "bypassPermissions", "--max-turns", MAX_TURNS]
    if resume:
        cmd += ["--resume", resume]
    with transcript.open("a") as out, transcript.with_suffix(".err").open("a") as err:
        proc = subprocess.Popen(cmd, cwd=DATA / "workspace", env=agent_env(run_id), stdout=subprocess.PIPE,
                                stderr=err, text=True, start_new_session=True, **as_agent())
        RUN.update(proc=proc, stopped=False, timed_out=False)

        def on_timeout():
            RUN["timed_out"] = True
            kill_group(proc)

        timer = threading.Timer(JOB_TIMEOUT, on_timeout)
        timer.start()
        try:
            for line in proc.stdout:
                if '"thinking_tokens"' not in line[:200]:  # skip the token-count ticker
                    out.write(line)
            proc.wait()
        finally:
            timer.cancel()
            RUN["proc"] = None
    give_to_agent(transcript)
    with transcript.open() as f:
        f.seek(start)
        steps, result, session = summarize_stream(f)
    if RUN["stopped"] or STOP.is_set():
        return "", session or resume, "stopped", steps, None
    if RUN["timed_out"]:
        return f"⚠️ Gave up after {JOB_TIMEOUT / 60:.0f} minutes without finishing.", session or resume, "timed out", steps, None
    if result is None:
        error = transcript.with_suffix(".err").read_text(errors="replace")[-600:].strip()
        if resume and (not steps) and re.search(r"no conversation found|session", error, re.I):
            log("resume failed, retrying fresh:", error[-300:])  # e.g. the saved session is gone
            return run_claude(prompt, run_id=run_id)
        return f"⚠️ Claude Code exited with {proc.returncode}: {error}", session, "failed", steps, None
    usage = {**(result.get("usage") or {}), "num_turns": result.get("num_turns"), "duration_ms": result.get("duration_ms")}
    usage = {k: v for k, v in usage.items() if k in ("input_tokens", "cache_read_input_tokens", "output_tokens",
                                                     "num_turns", "duration_ms")}
    session = result.get("session_id") or session
    if result.get("is_error") or result.get("subtype") != "success":
        text = result.get("result") or result.get("subtype") or "Claude Code reported an error"
        return f"⚠️ {text}", session, "failed", steps, usage
    return result.get("result") or "", session, "done", steps, usage


def run_precheck(command):
    try:
        done = subprocess.run(
            ["bash", "-c", command], cwd=DATA / "workspace", env=agent_env(),
            capture_output=True, text=True, timeout=300, **as_agent(),
        )
        return done.returncode, done.stdout + done.stderr
    except subprocess.TimeoutExpired:
        return 124, "precheck timed out after 5 minutes"


def chat_session():
    try:
        saved = json.loads(SESSION_FILE.read_text())
    except (OSError, ValueError):
        return None
    return saved["id"] if time.time() - saved["at"] < CHAT_IDLE_SECONDS else None


def respond(kind, label, prompt, silent_ok, conversation=True, typing_for=None):
    """One recorded orchestrator turn. A turn that fails on a transient model/API error is retried
    by resuming its session, so it carries on rather than starting over. The reply is queued for
    the owner unless the orchestrator chose SILENT."""
    run_id = start_run(kind, label, prompt)
    resume = chat_session() if conversation else None
    typing = keep_typing(typing_for) if typing_for else None
    all_steps, attempt, text = [], 0, prompt
    try:
        while True:
            attempt += 1
            reply, session, status, steps, usage = run_claude(text, resume, run_id)
            all_steps += steps
            if conversation and session:
                SESSION_FILE.write_text(json.dumps({"id": session, "at": time.time()}))
            if status != "failed" or attempt >= TURN_ATTEMPTS or not transient(reply) or STOP.is_set():
                break
            log(f"run {run_id}: transient failure, retrying ({attempt}/{TURN_ATTEMPTS}):", clip(reply, 200))
            time.sleep(RETRY_SECONDS * attempt * attempt)
            if session:
                resume = session
                text = (f"[Your previous attempt stopped with an error: {clip(reply, 300)} Carry on from where you "
                        "left off, without redoing steps that already succeeded.]")
    except Exception as e:
        finish_run(run_id, "failed", f"bridge error: {e!r}", attempts=attempt)
        raise
    finally:
        if typing:
            typing.set()
    silent = silent_ok and status == "done" and reply.strip().strip(".").upper() == "SILENT"
    transcript = str(DATA / "runs" / f"{run_id}.jsonl") if run_id else None
    finish_run(run_id, "silent" if silent else status, reply, session, all_steps[-300:], usage, attempt,
               {"transcript": transcript})
    if not silent and status != "stopped":
        queue_message(reply, run_id=run_id)


def handle(job):
    note = ""
    if job.get("resumed"):
        note = "[The machine restarted while this was running. Check what was already done before repeating anything.]\n"
    if job["kind"] == "messages":
        last = job["messages"][-1]["id"]
        try:
            wa("POST", "/statuses", {"messaging_product": "whatsapp", "status": "read", "message_id": last,
                                     "typing_indicator": {"type": "text"}})
        except OSError:
            pass
        prompt = note + f"[WhatsApp · {now_str()}]\n" + "\n".join(describe(m) for m in job["messages"])
        respond("message", None, prompt, silent_ok=False, typing_for=last)
    elif job["kind"] == "report":
        respond("report", job["id"], note + job["prompt"], silent_ok=True)
    else:  # a scheduled task, in a session of its own
        extra = ""
        if job.get("precheck"):
            code, output = run_precheck(job["precheck"])
            if code == 0 and not output.strip():
                log(f"task {job['name']!r}: precheck found nothing, skipped")
                return
            extra = f"\n\n[precheck exited {code}; output:]\n{output[-4000:]}"
        prompt = note + f"[Scheduled task “{job['name']}” · {now_str()}]\n{job['prompt']}{extra}"
        respond("task", job["name"], prompt, silent_ok=True, conversation=False)


def mark_seen(key):
    path = DATA / "state" / "seen" / hashlib.sha256(key.encode()).hexdigest()
    try:
        os.close(os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY))
        return True
    except FileExistsError:
        return False


def enqueue(job):
    tmp = DATA / "queue" / f".{uuid.uuid4().hex}.tmp"
    tmp.write_text(json.dumps(job))
    tmp.rename(DATA / "queue" / f"{time.time_ns()}.json")


def queued_jobs():
    jobs = []
    for path in sorted((DATA / "queue").glob("*.json")):
        try:
            jobs.append((path, json.loads(path.read_text())))
        except (OSError, ValueError):
            continue
    return jobs


def cancel_worker_jobs():
    sql("update agent.runs set status = 'cancelled', finished_at = now() where kind = 'worker' and status = 'queued'")
    sql("update agent.runs set status = 'cancel_requested' where kind = 'worker' and status = 'running'")


def stop_all():
    dropped = 0
    for queued, _ in queued_jobs():
        queued.unlink(missing_ok=True)
        dropped += 1
    try:
        cancel_worker_jobs()
    except Exception as e:  # noqa: BLE001
        log("couldn't cancel worker jobs:", repr(e))
    proc = RUN["proc"]
    if proc:
        RUN["stopped"] = True
        kill_group(proc)
    queue_message("Stopped everything." + (f" Dropped {dropped} queued job(s)." if dropped else ""))


def status_text():
    """What /status reports, without involving the model."""
    job = RUN["job"]
    lines = [f"Working on: {job['kind']}{' “' + job['label'] + '”' if job.get('label') else ''}, since {job['since']}"
             if job else "Orchestrator: idle"]
    waiting = [j["kind"] for _, j in queued_jobs()]
    if waiting:
        lines.append(f"Queued: {len(waiting)} ({', '.join(sorted(set(waiting)))})")
    try:
        for rid, label, status, progress in sql(
                "select id, label, status, progress from agent.runs where kind = 'worker' "
                "and status in ('running', 'cancel_requested', 'queued') order by id"):
            lines.append(f"Worker run {rid} ({status}){': ' + clip(progress, 160) if progress else ''}")
        upcoming = sql("select name, next_run_at from agent.schedules where enabled and next_run_at is not null "
                       "order by next_run_at limit 5")
        if upcoming:
            lines.append("Next: " + "; ".join(f"{n} {t.astimezone().strftime('%a %H:%M')}" for n, t in upcoming))
    except Exception as e:  # noqa: BLE001
        lines.append(f"(database unavailable: {e})")
    return "\n".join(lines)


def accept(job):
    """Persists a job (crash-safe) after de-duplicating it; handles /stop, /new and /status."""
    if job["user"]:
        USER_FILE.write_text(job["user"])
    if job["kind"] != "messages":
        if mark_seen(job["id"]):
            enqueue(job)
        return
    fresh = []
    for msg in job["messages"]:
        if not mark_seen(msg["id"]):
            continue
        command = msg["text"]["body"].strip().lower() if msg.get("type") == "text" else ""
        if command == "/stop":
            stop_all()
        elif command == "/new":
            SESSION_FILE.unlink(missing_ok=True)
            queue_message("Okay, starting a fresh conversation.")
        elif command == "/status":
            queue_message(status_text())
        else:
            fresh.append(msg)
    if fresh:
        enqueue({"kind": "messages", "user": job["user"], "messages": fresh})


def take_next_job():
    """The owner's messages first (all queued ones merged into one turn, once they've paused for
    a couple of seconds), then worker reports, then scheduled tasks."""
    jobs = queued_jobs()
    if not jobs:
        return None, None
    jobs.sort(key=lambda j: (PRIORITY.get(j[1]["kind"], 9), j[0].name))
    path, job = jobs[0]
    running = DATA / "queue" / "running" / path.name
    if job["kind"] != "messages":
        path.rename(running)
        return running, job
    group = [j for j in jobs if j[1]["kind"] == "messages"]
    try:
        if time.time() - max(p.stat().st_mtime for p, _ in group) < 2:
            return None, None
    except FileNotFoundError:
        return None, None
    merged = {"kind": "messages", "user": group[-1][1]["user"], "messages": [m for _, j in group for m in j["messages"]]}
    if any(j.get("resumed") for _, j in group):
        merged["resumed"] = True
    running.write_text(json.dumps(merged))
    for p, _ in group:
        p.unlink(missing_ok=True)
    return running, merged


def run_jobs(threads):
    while not STOP.is_set():
        dead = [t.name for t in threads if not t.is_alive()]
        if dead:  # exit so Fly restarts the machine rather than limping on without, say, the poller
            log("thread died, restarting:", ", ".join(dead))
            os._exit(1)
        try:
            running, job = take_next_job()
        except (OSError, ValueError) as e:  # e.g. a file /stop removed under us
            log("queue:", repr(e))
            running = job = None
        if not job:
            time.sleep(0.5)
            continue
        RUN["job"] = {"kind": job["kind"], "label": job.get("name") or (job["id"] if job["kind"] == "report" else None),
                      "since": datetime.now().astimezone().strftime("%H:%M")}
        try:
            handle(job)
        except Exception as e:  # noqa: BLE001 - one bad job must not take the bridge down
            log("job failed:", repr(e))
            queue_message(f"⚠️ The bridge hit an error on that job: {e!r}")
        finally:
            RUN["job"] = None
            if not STOP.is_set():  # if we're being shut down, leave it to be resumed on next boot
                running.unlink(missing_ok=True)


# ---------------------------------------------------------------- WhatsApp poller + schedule clock


def ensure_tables():
    sql((APP / "supabase" / "tables.sql").read_text())  # idempotent: new tables ship with a deploy
    TABLES_READY.set()


def fire_due_schedules():
    from croniter import croniter

    rows = sql(
        "select id, name, cron, tz, prompt, precheck, run_once, next_run_at from agent.schedules "
        "where enabled and (next_run_at is null or next_run_at <= now()) order by next_run_at nulls first"
    )
    busy = {j.get("name") for _, j in queued_jobs() if j["kind"] == "task"}
    if RUN["job"] and RUN["job"]["kind"] == "task":
        busy.add(RUN["job"]["label"])
    for sid, name, cron, tz, prompt, precheck, run_once, due in rows:
        try:
            following = croniter(cron, datetime.now(ZoneInfo(tz))).get_next(datetime)
        except Exception as e:  # noqa: BLE001 - a bad cron or timezone written by the agent
            sql("update agent.schedules set enabled = false, last_error = %s where id = %s", f"disabled: {e}", sid)
            queue_message(f"⚠️ Schedule “{name}” was disabled: {e}")
            continue
        if due is None:  # a new row: arm it for its first run
            sql("update agent.schedules set next_run_at = %s, last_error = null where id = %s", following, sid)
        elif not owner():
            continue
        elif name in busy:  # the previous run hasn't happened yet; don't pile up
            log(f"schedule {name!r} skipped: its previous run is still queued or running")
            sql("update agent.schedules set next_run_at = %s where id = %s", following, sid)
        else:
            accept({"kind": "task", "user": owner(), "id": f"task:{sid}:{due.isoformat()}",
                    "name": name, "prompt": prompt, "precheck": precheck})
            sql("update agent.schedules set last_run_at = now(), next_run_at = %s, enabled = %s where id = %s",
                following, not run_once, sid)


def load_offset():
    offset = get_state("wa_offset")
    if offset is None:  # first start: read from the beginning, but ignore history older than now
        since = int(time.time())
        set_state("wa_since", since)
        set_state("wa_offset", 0)
        return 0, since
    return int(offset), int(get_state("wa_since") or 0)


def poll_loop():
    heartbeat_url = os.environ.get("HEALTHCHECK_URL")
    while True:
        try:
            ensure_tables()
            offset, since = load_offset()
            break
        except Exception as e:  # noqa: BLE001 - e.g. Postgres unreachable at boot; keep trying
            log("can't set up Postgres yet:", repr(e))
            time.sleep(15)
    last_tick = last_beat = 0.0
    log("polling WhatsApp")
    while not STOP.is_set():
        try:
            now = time.time()
            if now - last_tick >= 60:
                last_tick = now
                fire_due_schedules()
            if heartbeat_url and now - last_beat >= 300:
                last_beat = now
                http("GET", heartbeat_url, timeout=10)
            status, response = wa("GET", f"/updates?offset={offset}&limit=50&timeout=25", timeout=60)
            if status == 204:
                continue
            if status != 200:
                log("poll:", status, response[:200])
                time.sleep(60 if status in (400, 401, 403) else 5)
                continue
            data = json.loads(response)
            value = data["entry"][0]["changes"][0]["value"]
            messages = [
                m for m in value.get("messages", [])
                if m.get("from", "").startswith("user:") and int(m.get("timestamp", 0)) >= since
            ]
            if messages:
                record_inbound(messages)
                accept({"kind": "messages", "user": messages[-1]["from"], "messages": messages})
            offset = data["next_offset"]
            set_state("wa_offset", offset)
        except Exception as e:  # noqa: BLE001 - network or database hiccup; keep polling
            log("poller error:", repr(e))
            time.sleep(5)


# ---------------------------------------------------------------- the worker sprite


def sprites(method, path, body=None, query=None, headers=None, retry=True, allow_missing=False):
    """Calls the Sprites API. Retries only when told the call is safe to replay."""
    url = SPRITES_API + path + (f"?{urllib.parse.urlencode(query, doseq=True)}" if query else "")
    headers = {**(headers or {}), "Authorization": f"Bearer {SPRITES_TOKEN}"}
    for attempt in range(1, 5):
        try:
            status, response = http(method, url, body, headers, timeout=120)
        except OSError:
            if not retry or attempt == 4:
                raise
            time.sleep(min(0.5 * 2**attempt, 5))
            continue
        if status < 300:
            return response
        if allow_missing and status == 404:
            return None
        if retry and status in (429, 500, 502, 503, 504) and attempt < 4:
            time.sleep(min(0.5 * 2**attempt, 5))
            continue
        raise RuntimeError(f"Sprites API {method} {path} failed with {status}: {response[:300]!r}")


def sprite_write(path, content):
    data = content.encode() if isinstance(content, str) else content
    sprites("PUT", f"/sprites/{SPRITE}/fs/write", data, {"path": path, "mkdir": "true"},
            {"Content-Type": "application/octet-stream"})


def sprite_read(path):
    return sprites("GET", f"/sprites/{SPRITE}/fs/read", query={"path": path}, allow_missing=True)


def sprite_sh(script, retry=True):
    """Runs a bash script in the sprite. The exec response isn't framed reliably, so scripts
    write anything we need into files that are read back."""
    sprites("POST", f"/sprites/{SPRITE}/exec", query=[("cmd", "bash"), ("cmd", "-lc"), ("cmd", script)],
            retry=retry)


def ensure_sprite():
    """Creates the one worker sprite on first use; afterwards it just wakes on demand."""
    if sprites("GET", f"/sprites/{SPRITE}", allow_missing=True) is not None:
        return
    try:
        sprites("POST", "/sprites", {"name": SPRITE, "wait_for_capacity": True})
    except RuntimeError as e:
        if "failed with 409" not in str(e):  # 409: it already exists
            raise


RESIDENTIAL_KEYS = ("BYTEFUL_PUBLIC_KEY", "BYTEFUL_PRIVATE_KEY")  # paid; only for jobs flagged --residential


def worker_env(residential=False, run_id=None):
    """Exports for the worker's Claude: written per job, sourced and deleted by run.sh."""
    keep = ("ANTHROPIC_", "CLAUDE_CODE_", "DISABLE_", "AGENT_")
    names = sorted(k for k in os.environ if k.startswith(keep) or k in ("API_TIMEOUT_MS", "DATABASE_URL", "TZ"))
    if residential:
        names += [k for k in RESIDENTIAL_KEYS if k in os.environ]
    lines = [f"export {name}={shlex.quote(os.environ[name])}" for name in names]
    lines += [f"export RUN_ID={shlex.quote(str(run_id or ''))}", f"export WORKER_TIMEOUT_SECONDS={int(WORKER_TIMEOUT)}",
              'export PATH="$HOME/.local/bin:$PATH"']
    return "\n".join(lines) + "\n"


def start_worker_job(job_dir, brief, residential=False, run_id=None):
    ensure_sprite()
    job = f"{SPRITE_ROOT}/jobs/{job_dir}"
    for name in ("CLAUDE.md", "mcp.json", "provision.sh", "run.sh", "status.py"):
        sprite_write(f"{SPRITE_ROOT}/{name}", (APP / "worker" / name).read_bytes())
    sprite_write(f"{SPRITE_ROOT}/.env", worker_env(residential, run_id))
    if residential:
        brief += "\n\n[The owner authorized a residential IP (Byteful) for this job; its keys are in your environment.]"
    sprite_write(f"{job}/BRIEF.md", brief)
    # Not retried: a failure can arrive after the job has started. The caller checks `launched`.
    sprite_sh(
        f"sprite-env curl -X PUT /v1/tasks/job-{job_dir} -d '{{\"expire\":\"5m\"}}' > /dev/null 2>&1; "
        f"setsid -f nohup bash {SPRITE_ROOT}/run.sh {job} > /dev/null 2>&1 < /dev/null; "
        f"echo ok > {job}/launched",
        retry=False,
    )
    if sprite_read(f"{job}/launched") is None:
        raise RuntimeError("the job didn't launch")


def worker_status(job_dir, full=False):
    job = f"{SPRITE_ROOT}/jobs/{job_dir}"
    out = f"{job}/status{'-full' if full else ''}.json"
    sprite_sh(f"python3 {SPRITE_ROOT}/status.py {job}{' full' if full else ''} > {out}.tmp 2>&1; mv {out}.tmp {out}")
    raw = sprite_read(out)
    return json.loads(raw) if raw else None


def cancel_worker_job(job_dir):
    """Marks the job cancelled; its launcher stops the agent. A job whose launcher died is ended here."""
    job = f"{SPRITE_ROOT}/jobs/{job_dir}"
    sprite_sh(f"cd {job} 2> /dev/null || exit 0; [ -f exit_code ] && exit 0; : > cancelled; "
              f"pid=$(cat launcher.pid 2> /dev/null) && grep -qaF {job} /proc/$pid/cmdline 2> /dev/null "
              f"|| echo 143 > exit_code")


def report(job, state, text, steps=None, usage=None):
    """Ends the worker run and hands its report to the orchestrator as a new job."""
    finish_run(job["id"], state, text, steps=steps, usage=usage)
    header = f"[Worker report · job {job['label']} · run {job['id']} · {state}]"
    accept({"kind": "report", "user": owner(), "id": f"report:{job['id']}",
            "prompt": f"{header}\nBrief: {clip(job['input'], 300)}\n\n{text}"})


def collect_worker_job(job, status):
    """Copies the job's output/ files and step log here, and hands the report to the orchestrator."""
    full = None
    try:
        full = worker_status(job["meta"]["dir"], full=True)
    except Exception as e:  # noqa: BLE001 - the report matters more than the step log
        log("worker step log unavailable:", repr(e))
    status = full or status
    local = DATA / "worker" / job["label"]
    local.mkdir(parents=True, exist_ok=True)
    give_to_agent(local)
    names = []
    for name in status.get("outputs", [])[:20]:
        data = sprite_read(f"{SPRITE_ROOT}/jobs/{job['meta']['dir']}/output/{name}")
        if data is None or len(data) > 25 * 2**20:
            continue
        path = local / Path(name).name
        path.write_bytes(data)
        give_to_agent(path)
        names.append(path.name)
    if status.get("steps"):
        steps_file = local / "steps.json"
        steps_file.write_text(json.dumps(status["steps"], indent=1))
        give_to_agent(steps_file)
    state = ("cancelled" if status.get("cancelled") else "timed out" if status.get("timed_out")
             else "finished" if status.get("exit_code") == 0 else "failed")
    text = status.get("final_message") or "(The worker sent no final message.)"
    if names:
        text += f"\n\nFiles copied to {local}/: {', '.join(names)}"
    if status.get("steps"):
        text += f"\nIts step-by-step log: {local}/steps.json"
    if state != "finished" and status.get("log_tail"):
        text += f"\n\n[worker error output]\n{status['log_tail'][-1500:]}"
    report(job, state, text, steps=(status.get("steps") or [])[-300:], usage=status.get("usage"))


def worker_jobs(statuses, extra=""):
    rows = sql("select id, label, input, meta, attempts, started_at, status from agent.runs "
               f"where kind = 'worker' and status = any(%s) {extra} order by id limit 1", list(statuses))
    if not rows:
        return None
    rid, label, text, meta, attempts, started_at, status = rows[0]
    return {"id": rid, "label": label, "input": text, "meta": meta or {}, "attempts": attempts,
            "started_at": started_at, "status": status}


def requeue_worker_job(job, reason):
    """Puts a job back in the queue with backoff, or gives up after WORKER_ATTEMPTS."""
    if job["attempts"] >= WORKER_ATTEMPTS:
        report(job, "failed", f"Gave up after {job['attempts']} attempts. Last problem: {reason}")
        return
    delay = RETRY_SECONDS * 2 * job["attempts"] ** 2
    log(f"worker run {job['id']}: retrying in {delay:.0f}s ({reason})")
    sql("update agent.runs set status = 'queued', meta = meta || %s::jsonb where id = %s",
        jsonb({"retry_after": time.time() + delay, "last_problem": clip(reason, 500)}), job["id"])


def start_next_worker_job():
    job = worker_jobs(["queued"], "and coalesce((meta->>'retry_after')::float8, 0) <= extract(epoch from now())")
    if not job:
        return
    job["attempts"] += 1
    job["meta"]["dir"] = job["label"] if job["attempts"] == 1 else f"{job['label']}-{job['attempts']}"
    sql("update agent.runs set status = 'running', attempts = %s, started_at = now(), progress = null, "
        "meta = meta || %s::jsonb where id = %s", job["attempts"], jsonb({"dir": job["meta"]["dir"]}), job["id"])
    try:
        start_worker_job(job["meta"]["dir"], job["input"], job["meta"].get("residential") is True, job["id"])
    except Exception as e:  # noqa: BLE001
        try:  # a launch that errored may still have started; if so, follow it rather than start it twice
            if sprite_read(f"{SPRITE_ROOT}/jobs/{job['meta']['dir']}/launched") is not None:
                return
        except Exception:  # noqa: BLE001
            pass
        log("worker start failed:", repr(e))
        requeue_worker_job(job, f"couldn't start the worker: {e}")


def follow_worker_job(job):
    """Checks a running job: collects it when done, retries infrastructure failures, gives up on a
    job that stopped responding."""
    try:
        status = worker_status(job["meta"]["dir"])
    except Exception as e:  # noqa: BLE001 - the API is flaky; keep following the job
        log("worker status:", repr(e))
        status = {"state": "unknown"}
    overdue = (datetime.now().astimezone() - job["started_at"]).total_seconds() > WORKER_TIMEOUT + 900
    if status is None:
        requeue_worker_job(job, "the job's files disappeared from the worker")
    elif status["state"] == "finished":
        if status.get("exit_code") == 97 and not status.get("cancelled"):  # provisioning failed: retry
            requeue_worker_job(job, f"setting up the worker failed: {clip(status.get('log_tail'), 300)}")
        else:
            collect_worker_job(job, status)
    elif overdue:
        try:
            cancel_worker_job(job["meta"]["dir"])
        except Exception:  # noqa: BLE001
            pass
        report(job, "unresponsive", "The worker stopped responding, so the job was abandoned.")
    elif status.get("progress"):
        sql("update agent.runs set progress = %s where id = %s", clip(status["progress"], 500), job["id"])


def worker_loop():
    """One job at a time on the one sprite. Jobs queue in agent.runs (kind 'worker'), which the
    `delegate` command writes; this starts them, follows them every 30s, and collects them."""
    TABLES_READY.wait()
    last_check = 0.0
    while not STOP.is_set():
        try:
            job = worker_jobs(["running", "cancel_requested"])
            if job and job["status"] == "cancel_requested" and not job["meta"].get("cancel_sent"):
                cancel_worker_job(job["meta"]["dir"])
                sql("update agent.runs set meta = meta || '{\"cancel_sent\": true}' where id = %s", job["id"])
                last_check = 0.0
            if job and time.time() - last_check >= 30:
                last_check = time.time()
                follow_worker_job(job)
            elif not job:
                start_next_worker_job()
                last_check = time.time()
        except Exception as e:  # noqa: BLE001
            log("worker loop:", repr(e))
        time.sleep(5)


# ---------------------------------------------------------------- startup


def prepare():
    for sub in ("queue/running", "state/seen", "inbox", "runs", "worker", "workspace", "home/.claude"):
        (DATA / sub).mkdir(parents=True, exist_ok=True)
    for name in ("CLAUDE.md", "settings.json"):  # infrastructure-owned; refreshed on every boot
        shutil.copy(APP / "agent" / name, DATA / "home" / ".claude" / name)
    if os.geteuid() == 0:
        for sub in ("inbox", "runs", "worker", "workspace", "home", "home/.claude",
                    "home/.claude/CLAUDE.md", "home/.claude/settings.json"):
            os.chown(DATA / sub, *agent_ids())
    now = time.time()
    for folder, days in (("state/seen", 35), ("runs", 90)):  # WhatsApp keeps updates for 30 days
        for item in (DATA / folder).iterdir():
            if item.is_file() and item.stat().st_mtime < now - days * 86400:
                item.unlink()
    for interrupted in (DATA / "queue" / "running").glob("*.json"):
        job = json.loads(interrupted.read_text())
        job["resumed"] = True
        (DATA / "queue" / interrupted.name).write_text(json.dumps(job))
        interrupted.unlink()


def on_signal(*_):
    STOP.set()
    proc = RUN["proc"]
    if proc:
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass


def main():
    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    prepare()
    threads = [threading.Thread(target=loop, name=loop.__name__, daemon=True)
               for loop in (poll_loop, sender_loop, worker_loop)]
    for thread in threads:
        thread.start()
    log("orchestrator ready")
    run_jobs(threads)
    log("stopped")


if __name__ == "__main__":
    main()
