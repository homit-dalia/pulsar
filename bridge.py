#!/usr/bin/env python3
"""Plumbing for a personal agent: WhatsApp <-> an orchestrator Claude, plus one worker Sprite.

A single process on one small, always-on Fly Machine:
  * long-polls the WhatsApp Agent Platform and queues the owner's messages
  * fires due rows of agent.schedules (Postgres)
  * runs the orchestrator (`claude -p`), one job at a time
  * runs what the orchestrator `delegate`s on one Fly Sprite, which has its own Claude and a
    browser and sleeps when idle; each job's report comes back to the orchestrator as a new job

No agent logic lives here: the agents' instructions are agent/CLAUDE.md and worker/CLAUDE.md.

WhatsApp Agent Platform: https://www.whatsapp.com/developer/WhatsApp-Agent-Platform-Developer-Manual.pdf
Sprites API: https://docs.sprites.dev
"""
from __future__ import annotations

import hashlib
import json
import mimetypes
import os
import pwd
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

STOP = threading.Event()
RUN = {"proc": None, "stopped": False}
SESSION_FILE = DATA / "state" / "chat_session.json"
USER_FILE = DATA / "state" / "wa_user"
WORKER_STATE = DATA / "state" / "worker.json"


def log(*parts):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), *parts, flush=True)


def now_str():
    return datetime.now().astimezone().strftime("%a %Y-%m-%d %H:%M %Z")


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


# ---------------------------------------------------------------- WhatsApp

SEND_LOCK = threading.Lock()  # the API asks for no concurrent sends to one recipient


def wa(method, path, body=None, headers=None, timeout=60):
    headers = {**(headers or {}), "Authorization": f"Bearer {WA_TOKEN}"}
    return http(method, WA_API + path, body, headers, timeout)


def _send(payload):
    """POST /messages with the manual's retry rules. Returns the message id, or None."""
    for attempt in range(4):
        try:
            status, response = wa("POST", "/messages", payload, timeout=120)
        except OSError as e:  # outcome unknown; retrying could send it twice
            log("send error:", e)
            return None
        if status == 200:
            return json.loads(response)["messages"][0]["id"]
        if status in (429, 503):
            time.sleep(5 * 2**attempt)
            continue
        log("send failed:", status, response[:300])
        return None
    return None


def record_outbound(wamid, text):
    """Log what the owner was sent, so the orchestrator can resolve quoted replies later."""
    stamp = datetime.now().astimezone().isoformat(timespec="seconds")
    with (DATA / "state" / "outbox.log").open("a") as f:
        f.write(f"{stamp}\t{wamid or 'NOT-SENT'}\t{text[:2000]!r}\n")


def send_text(user, text):
    text = (text or "").strip()
    if not user or not text:
        return
    with SEND_LOCK:
        for i in range(0, len(text), 4000):
            chunk = text[i : i + 4000]
            payload = {"messaging_product": "whatsapp", "to": user, "type": "text", "text": {"body": chunk}}
            record_outbound(_send(payload), chunk)


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
    status, response = wa("POST", "/media", body, content_type, timeout=300)
    if status != 200:
        log("upload failed:", status, response[:300])
        send_text(user, f"(Couldn't send {name}: upload failed with HTTP {status}.)")
        return
    media = {"id": json.loads(response)["id"]}
    if caption and kind != "audio":
        media["caption"] = caption[:1024]
    if kind == "document":
        media["filename"] = name
    with SEND_LOCK:
        wamid = _send({"messaging_product": "whatsapp", "to": user, "type": kind, kind: media})
        record_outbound(wamid, f"[file {name}] {caption}")


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


# ---------------------------------------------------------------- orchestrator jobs


def agent_ids():
    entry = pwd.getpwnam(AGENT_USER)
    return entry.pw_uid, entry.pw_gid


def as_agent():
    """Popen kwargs that drop root to the agent user (Claude Code won't bypass permissions as root)."""
    return {"user": AGENT_USER, "group": AGENT_USER, "extra_groups": []} if os.geteuid() == 0 else {}


def give_to_agent(path):
    if os.geteuid() == 0:
        os.chown(path, *agent_ids())


def agent_env():
    """The orchestrator's environment: model settings, the database and AGENT_* credentials only."""
    keep = ("ANTHROPIC_", "CLAUDE_CODE_", "DISABLE_", "AGENT_")
    env = {k: v for k, v in os.environ.items() if k.startswith(keep)}
    env.update({k: os.environ[k] for k in ("API_TIMEOUT_MS", "DATABASE_URL", "TZ") if k in os.environ})
    home = DATA / "home"
    env.update(
        HOME=str(home), USER=AGENT_USER, LOGNAME=AGENT_USER, SHELL="/bin/bash", LANG="C.UTF-8",
        DATA_DIR=str(DATA), PATH=f"{home}/.local/bin:/usr/local/bin:/usr/bin:/bin",
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


def run_claude(prompt, resume=None):
    """Runs one orchestrator turn. Returns (reply text, session id)."""
    cmd = [CLAUDE_BIN, "-p", prompt, "--output-format", "json", "--permission-mode", "bypassPermissions",
           "--max-turns", MAX_TURNS]
    if resume:
        cmd += ["--resume", resume]
    proc = subprocess.Popen(
        cmd, cwd=DATA / "workspace", env=agent_env(), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, start_new_session=True, **as_agent(),
    )
    RUN["proc"], RUN["stopped"] = proc, False
    try:
        out, err = proc.communicate(timeout=JOB_TIMEOUT)
    except subprocess.TimeoutExpired:
        kill_group(proc)
        proc.communicate()
        return f"⚠️ Gave up after {JOB_TIMEOUT / 60:.0f} minutes without finishing.", resume
    finally:
        RUN["proc"] = None
    if RUN["stopped"] or STOP.is_set():
        return "", resume
    try:
        result = json.loads(out)
    except ValueError:
        try:
            result = json.loads(out.strip().splitlines()[-1])
        except (ValueError, IndexError):
            if resume:  # e.g. the saved session can't be resumed; start a fresh one once
                log("resume failed, retrying fresh:", (err or out)[-300:])
                return run_claude(prompt)
            return f"⚠️ Claude Code exited with {proc.returncode}: {(err or out).strip()[-600:]}", None
    session = result.get("session_id")
    if result.get("is_error") or result.get("subtype") != "success":
        return f"⚠️ {result.get('result') or result.get('subtype') or 'Claude Code reported an error'}", session
    return result.get("result") or "", session


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


def converse(prompt):
    """A turn in the ongoing conversation with the owner (messages and worker reports)."""
    reply, session = run_claude(prompt, resume=chat_session())
    if session:
        SESSION_FILE.write_text(json.dumps({"id": session, "at": time.time()}))
    return reply


def drain_outbox(limit=15):
    """Let the run's `notify` messages go out before its final reply."""
    end = time.time() + limit
    while any((DATA / "outbox").glob("*.json")) and time.time() < end:
        time.sleep(0.5)


def deliver(user, reply, silent_ok):
    if STOP.is_set() or (silent_ok and reply.strip().strip(".").upper() == "SILENT"):
        return
    drain_outbox()
    send_text(user, reply)


def handle(job):
    user = job["user"]
    note = ""
    if job.get("resumed"):
        note = "[The machine restarted while this was running. Check what was already done before repeating anything.]\n"
    if job["kind"] == "messages":
        read = {"messaging_product": "whatsapp", "status": "read", "message_id": job["messages"][-1]["id"],
                "typing_indicator": {"type": "text"}}
        try:
            wa("POST", "/statuses", read)
        except OSError:
            pass
        prompt = note + f"[WhatsApp · {now_str()}]\n" + "\n".join(describe(m) for m in job["messages"])
        deliver(user, converse(prompt), silent_ok=False)
    elif job["kind"] == "report":
        deliver(user, converse(note + job["prompt"]), silent_ok=True)
    else:  # a scheduled task, in a session of its own
        extra = ""
        if job.get("precheck"):
            code, output = run_precheck(job["precheck"])
            if code == 0 and not output.strip():
                log(f"task {job['name']!r}: precheck found nothing, skipped")
                return
            extra = f"\n\n[precheck exited {code}; output:]\n{output[-4000:]}"
        reply, _ = run_claude(note + f"[Scheduled task “{job['name']}” · {now_str()}]\n{job['prompt']}{extra}")
        deliver(user, reply, silent_ok=True)


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


def stop_all(user):
    dropped = 0
    for queued in (DATA / "queue").glob("*.json"):
        queued.unlink(missing_ok=True)
        dropped += 1
    (DATA / "delegate" / "cancel").touch()  # the worker loop cancels its job and drops queued ones
    proc = RUN["proc"]
    if proc:
        RUN["stopped"] = True
        kill_group(proc)
    send_text(user, "Stopped everything." + (f" Dropped {dropped} queued job(s)." if dropped else ""))


def accept(job):
    """Persists a job (crash-safe) after de-duplicating it; handles the /stop and /new commands."""
    user = job["user"]
    if user:
        USER_FILE.write_text(user)
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
            stop_all(user)
        elif command == "/new":
            SESSION_FILE.unlink(missing_ok=True)
            send_text(user, "Okay, starting a fresh conversation.")
        else:
            fresh.append(msg)
    if fresh:
        enqueue({"kind": "messages", "user": user, "messages": fresh})


def run_jobs(threads):
    while not STOP.is_set():
        dead = [t.name for t in threads if not t.is_alive()]
        if dead:  # exit so Fly restarts the machine rather than limping on without, say, the poller
            log("thread died, restarting:", ", ".join(dead))
            os._exit(1)
        queued = sorted((DATA / "queue").glob("*.json"))
        if not queued:
            time.sleep(1)
            continue
        running = DATA / "queue" / "running" / queued[0].name
        try:
            queued[0].rename(running)
        except FileNotFoundError:  # dropped by /stop
            continue
        job = json.loads(running.read_text())
        try:
            handle(job)
        except Exception as e:  # noqa: BLE001 - one bad job must not take the bridge down
            log("job failed:", repr(e))
            send_text(job.get("user"), f"⚠️ The bridge hit an error on that job: {e!r}")
        finally:
            if not STOP.is_set():  # if we're being shut down, leave it to be resumed on next boot
                running.unlink(missing_ok=True)


def outbox_loop():
    """Sends what the orchestrator queued with the `notify` command."""
    agent_uid = agent_ids()[0] if os.geteuid() == 0 else os.geteuid()
    while not STOP.is_set():
        for item in sorted((DATA / "outbox").glob("*.json")):
            user = owner()
            try:
                msg = json.loads(item.read_text())
                if msg.get("file"):
                    st = os.stat(msg["file"])
                    if st.st_uid != agent_uid and not st.st_mode & 0o004:  # only files the agent could read
                        raise PermissionError(f"{msg['file']} isn't readable by the agent")
                    send_file(user, msg["file"], msg.get("text", ""))
                else:
                    send_text(user, msg.get("text", ""))
            except Exception as e:  # noqa: BLE001
                log("notify failed:", repr(e))
                send_text(user, f"(notify failed: {e})")
            item.unlink(missing_ok=True)
        time.sleep(1)


# ---------------------------------------------------------------- WhatsApp poller + schedule clock

_db = None


def sql(query, *args):
    global _db
    import psycopg

    for attempt in range(2):
        try:
            if _db is None or _db.closed:
                _db = psycopg.connect(
                    os.environ["DATABASE_URL"], autocommit=True, prepare_threshold=None, connect_timeout=15
                )
            cursor = _db.execute(query, args)
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


def fire_due_schedules():
    from croniter import croniter

    rows = sql(
        "select id, name, cron, tz, prompt, precheck, run_once, next_run_at from agent.schedules "
        "where enabled and (next_run_at is null or next_run_at <= now()) order by next_run_at nulls first"
    )
    for sid, name, cron, tz, prompt, precheck, run_once, due in rows:
        try:
            following = croniter(cron, datetime.now(ZoneInfo(tz))).get_next(datetime)
        except Exception as e:  # noqa: BLE001 - a bad cron or timezone written by the agent
            sql("update agent.schedules set enabled = false, last_error = %s where id = %s", f"disabled: {e}", sid)
            send_text(owner(), f"⚠️ Schedule “{name}” was disabled: {e}")
            continue
        if due is not None and owner():
            accept({"kind": "task", "user": owner(), "id": f"task:{sid}:{due.isoformat()}",
                    "name": name, "prompt": prompt, "precheck": precheck})
            sql("update agent.schedules set last_run_at = now(), next_run_at = %s, enabled = %s where id = %s",
                following, not run_once, sid)
        elif due is None:  # a new row: arm it for its first run
            sql("update agent.schedules set next_run_at = %s, last_error = null where id = %s", following, sid)


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
            offset, since = load_offset()
            break
        except Exception as e:  # noqa: BLE001 - e.g. Postgres unreachable at boot; keep trying
            log("can't read the WhatsApp offset from Postgres yet:", repr(e))
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


def worker_env():
    """Exports for the worker's Claude: written per job, sourced and deleted by run.sh."""
    keep = ("ANTHROPIC_", "CLAUDE_CODE_", "DISABLE_", "AGENT_")
    names = sorted(k for k in os.environ if k.startswith(keep) or k in ("API_TIMEOUT_MS", "DATABASE_URL", "TZ"))
    lines = [f"export {name}={shlex.quote(os.environ[name])}" for name in names]
    lines += [f"export WORKER_TIMEOUT_SECONDS={int(WORKER_TIMEOUT)}", 'export PATH="$HOME/.local/bin:$PATH"']
    return "\n".join(lines) + "\n"


def start_worker_job(job_id, brief):
    ensure_sprite()
    job = f"{SPRITE_ROOT}/jobs/{job_id}"
    for name in ("CLAUDE.md", "mcp.json", "provision.sh", "run.sh", "status.py"):
        sprite_write(f"{SPRITE_ROOT}/{name}", (APP / "worker" / name).read_bytes())
    sprite_write(f"{SPRITE_ROOT}/.env", worker_env())
    sprite_write(f"{job}/BRIEF.md", brief)
    # Not retried: a failure can arrive after the job has started.
    sprite_sh(
        f"sprite-env curl -X PUT /v1/tasks/job-{job_id} -d '{{\"expire\":\"5m\"}}' > /dev/null 2>&1; "
        f"setsid -f nohup bash {SPRITE_ROOT}/run.sh {job} > /dev/null 2>&1 < /dev/null; "
        f"echo ok > {job}/launched",
        retry=False,
    )
    if sprite_read(f"{job}/launched") is None:
        raise RuntimeError("the job didn't launch")


def worker_status(job_id):
    job = f"{SPRITE_ROOT}/jobs/{job_id}"
    sprite_sh(f"python3 {SPRITE_ROOT}/status.py {job} > {job}/status.json.tmp 2>&1; "
              f"mv {job}/status.json.tmp {job}/status.json")
    raw = sprite_read(f"{job}/status.json")
    return json.loads(raw) if raw else None


def cancel_worker_job(job_id):
    """Marks the job cancelled; its launcher stops the agent. A job whose launcher died is ended here."""
    job = f"{SPRITE_ROOT}/jobs/{job_id}"
    sprite_sh(f"cd {job} 2> /dev/null || exit 0; [ -f exit_code ] && exit 0; : > cancelled; "
              f"pid=$(cat launcher.pid 2> /dev/null) && grep -qaF {job} /proc/$pid/cmdline 2> /dev/null "
              f"|| echo 143 > exit_code")


def report(job_id, text):
    accept({"kind": "report", "user": owner(), "id": f"report:{job_id}", "prompt": text})


def collect_worker_job(current, status):
    """Copies the job's output/ files here and hands the worker's report to the orchestrator."""
    job_id = current["id"]
    local = DATA / "worker" / job_id
    names = []
    for name in status.get("outputs", [])[:20]:
        data = sprite_read(f"{SPRITE_ROOT}/jobs/{job_id}/output/{name}")
        if data is None or len(data) > 25 * 2**20:
            continue
        local.mkdir(parents=True, exist_ok=True)
        give_to_agent(local)
        path = local / Path(name).name
        path.write_bytes(data)
        give_to_agent(path)
        names.append(path.name)
    state = ("cancelled" if status.get("cancelled") else "timed out" if status.get("timed_out")
             else "finished" if status.get("exit_code") == 0 else "failed")
    lines = [f"[Worker report · job {job_id} · {state}]", f"Brief: {current['brief'][:300]}", "",
             status.get("final_message") or "(The worker sent no final message.)"]
    if names:
        lines += ["", f"Files copied to {local}/: {', '.join(names)}"]
    if state != "finished" and status.get("log_tail"):
        lines += ["", "[worker error output]", status["log_tail"][-1500:]]
    report(job_id, "\n".join(lines))


def worker_loop():
    """One job at a time on the one sprite: start queued delegations, follow them, collect them."""
    current = json.loads(WORKER_STATE.read_text()) if WORKER_STATE.exists() else None
    last_check = 0.0
    while not STOP.is_set():
        try:
            cancel = DATA / "delegate" / "cancel"
            if cancel.exists():
                cancel.unlink()
                for pending in (DATA / "delegate").glob("*.json"):
                    pending.unlink()
                if current:
                    cancel_worker_job(current["id"])
                    last_check = 0.0
            if current and time.time() - last_check >= 30:
                last_check = time.time()
                try:
                    status = worker_status(current["id"])
                except Exception as e:  # noqa: BLE001 - the API is flaky; keep following the job
                    log("worker status:", repr(e))
                    status = {"state": "unknown"}
                if status is None:
                    report(current["id"], f"[Worker report · job {current['id']} · lost]\n"
                                          "The job's files are gone from the worker.")
                    current = None
                elif status["state"] == "finished":
                    collect_worker_job(current, status)
                    current = None
                elif time.time() - current["started"] > WORKER_TIMEOUT + 900:
                    try:
                        cancel_worker_job(current["id"])
                    except Exception:  # noqa: BLE001
                        pass
                    report(current["id"], f"[Worker report · job {current['id']} · unresponsive]\n"
                                          "The worker stopped responding, so the job was abandoned.")
                    current = None
                else:
                    current["progress"] = status.get("progress")
            if not current:
                pending = sorted((DATA / "delegate").glob("*.json"))
                if pending:
                    request = json.loads(pending[0].read_text())
                    pending[0].unlink()
                    try:
                        start_worker_job(request["id"], request["brief"])
                        current = {"id": request["id"], "brief": request["brief"], "started": time.time(),
                                   "progress": None}
                        last_check = time.time()
                    except Exception as e:  # noqa: BLE001
                        log("worker start failed:", repr(e))
                        report(request["id"], f"[Worker report · job {request['id']} · not started]\n"
                                              f"The worker couldn't be started: {e}")
        except Exception as e:  # noqa: BLE001
            log("worker loop:", repr(e))
        if current:
            WORKER_STATE.write_text(json.dumps(current))
        else:
            WORKER_STATE.unlink(missing_ok=True)
        time.sleep(5)


# ---------------------------------------------------------------- startup


def prepare():
    for sub in ("queue/running", "state/seen", "inbox", "outbox", "delegate", "worker", "workspace", "home/.claude"):
        (DATA / sub).mkdir(parents=True, exist_ok=True)
    for name in ("CLAUDE.md", "settings.json"):  # infrastructure-owned; refreshed on every boot
        shutil.copy(APP / "agent" / name, DATA / "home" / ".claude" / name)
    if os.geteuid() == 0:
        for sub in ("inbox", "outbox", "delegate", "worker", "workspace", "home", "home/.claude",
                    "home/.claude/CLAUDE.md", "home/.claude/settings.json"):
            os.chown(DATA / sub, *agent_ids())
    cutoff = time.time() - 35 * 86400  # WhatsApp keeps updates 30 days
    for marker in (DATA / "state" / "seen").iterdir():
        if marker.stat().st_mtime < cutoff:
            marker.unlink()
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
               for loop in (poll_loop, outbox_loop, worker_loop)]
    for thread in threads:
        thread.start()
    log("orchestrator ready")
    run_jobs(threads)
    log("stopped")


if __name__ == "__main__":
    main()
