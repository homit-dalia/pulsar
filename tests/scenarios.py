"""End-to-end scenarios against the bridge running in the test container (see tests/run.sh)."""
import json
import subprocess
import sys
import time
import urllib.request

WA = "http://127.0.0.1:18999"
USER = "user:50972923564215"
CONTAINER, PG = "agent-e2e", "agent-pg-test"
results, counter = [], [0]


def request(method, url, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read()
        return json.loads(raw) if raw else None


def sent():
    return request("GET", f"{WA}/_sent")


def texts():
    return [m["text"]["body"] for m in sent()["sent"] if m["type"] == "text"]


def inject(text=None, ts=None, msg_id=None, **extra):
    counter[0] += 1
    msg = {"from": USER, "id": msg_id or f"wamid.in{counter[0]}-{time.time()}", "timestamp": str(int(ts or time.time()))}
    if text is not None:
        msg.update(type="text", text={"body": text})
    msg.update(extra)
    request("POST", f"{WA}/_inject", [msg])
    return msg["id"]


def sh(cmd):
    return subprocess.run(["docker", "exec", CONTAINER, "bash", "-c", cmd], capture_output=True, text=True).stdout


def jsonl(path):
    return [json.loads(line) for line in sh(f"cat {path} 2>/dev/null").splitlines() if line.strip()]


def calls():
    return jsonl("/data/home/orch_calls.log")


def q(sql):
    cmd = ["docker", "exec", "-i", PG, "psql", "-U", "agent_rw", "-d", "postgres", "-At", "-c", sql]
    return subprocess.run(cmd, capture_output=True, text=True).stdout.strip()


def wait_for(pred, timeout=60, every=1.0):
    end = time.time() + timeout
    while time.time() < end:
        value = pred()
        if value:
            return value
        time.sleep(every)
    return None


def check(name, ok, detail=""):
    results.append(bool(ok))
    print(("PASS " if ok else "FAIL ") + name + (f"  [{str(detail)[:500]}]" if detail and not ok else ""), flush=True)


print("--- orchestrator turns")
inject("hello")
check("the orchestrator answers", wait_for(lambda: "echo: hello" in texts(), 40), texts())
first = calls()[0]
check("it runs as the agent user, without the bridge's secrets",
      first["uid"] == 1000 and not (first["has_sprites_token"] or first["has_wa_token"] or first["has_byteful"]), first)
run = q(f"select concat_ws('|', kind, status, attempts, jsonb_array_length(steps), usage->>'output_tokens', meta->>'transcript') "
        f"from agent.runs where id = {first['run_id']}")
check("the turn is recorded with its steps, token usage and transcript", run.startswith("message|done|1|1|50|/data/runs/"), run)
transcript = sh(f"cat /data/runs/{first['run_id']}.jsonl")
check("the transcript is kept, minus the token ticker", "echo checking" in transcript and "thinking_tokens" not in transcript)

inject("merge one")
time.sleep(0.3)
inject("merge two")
check("quick follow-up messages are merged into one turn",
      wait_for(lambda: [c for c in calls() if "merge one" in c["prompt"] and "merge two" in c["prompt"]], 40),
      [c["prompt"][-30:] for c in calls()])
wait_for(lambda: "echo: merge two" in texts(), 30)

inject("/status")
check("/status is answered without the model",
      wait_for(lambda: any(t.startswith(("Orchestrator: idle", "Working on")) for t in texts()), 20), texts()[-2:])

inject("MARKDOWN please")
check("Markdown is converted to WhatsApp formatting",
      wait_for(lambda: "*Done*\n*bold* and the link (https://example.com)" in texts(), 40), texts()[-1:])

inject("RETRYME please")
check("a send refused with a 503 is retried", wait_for(lambda: "RETRYME reply" in texts(), 40), texts()[-1:])
check("…and recorded as sent on the second attempt",
      q("select status || '|' || attempts from agent.messages where body = 'RETRYME reply'") == "sent|2",
      q("select status, attempts, error from agent.messages where body = 'RETRYME reply'"))

inject("FLAKY please")
check("a turn that hits a transient model error is retried",
      wait_for(lambda: any("Carry on from where you left off" in t for t in texts()), 60), texts()[-1:])
flaky = [c for c in calls() if "FLAKY" in c["prompt"] or "Carry on" in c["prompt"]]
check("…by resuming the same session in the same run",
      len(flaky) == 2 and flaky[1]["resume"] and flaky[1]["run_id"] == flaky[0]["run_id"], flaky)
check("…which records 2 attempts", q(f"select status || '|' || attempts from agent.runs where id = {flaky[0]['run_id']}") == "done|2")

inject(type="document", document={"id": "doc-123", "mime_type": "application/pdf", "sha256": "x",
                                  "filename": "resume.pdf", "caption": "my resume"})
got = wait_for(lambda: [c for c in calls() if "[document saved to" in c["prompt"]], 40)
check("an attachment is saved and described", got and "resume.pdf" in got[0]["prompt"] and 'caption "my resume"' in got[0]["prompt"], got)
check("…and owned by the agent", sh("stat -c %U /data/inbox/*resume.pdf").strip() == "agent")

inject("please NOTIFY me")
check("notify delivers text and files",
      wait_for(lambda: "progress update from the agent" in texts() and any(m["type"] == "image" for m in sent()["sent"]), 40))
wait_for(lambda: "echo: please NOTIFY me" in texts(), 30)
order = [m.get("text", {}).get("body", m["type"]) for m in sent()["sent"]]
check("…before the turn's final reply",
      "echo: please NOTIFY me" in order and order.index("progress update from the agent") < order.index("echo: please NOTIFY me"), order[-4:])
check("…with the run id on the queued rows",
      q("select count(*) from agent.messages where body = 'progress update from the agent' and run_id is not null") == "1")

inject("ancient message", ts=1000)
dup = f"wamid.dup-{time.time()}"
inject("dup check", msg_id=dup)
inject("dup check", msg_id=dup)
wait_for(lambda: "echo: dup check" in texts(), 30)
time.sleep(4)
check("history from before the first start is ignored", "echo: ancient message" not in texts())
check("a duplicate delivery is handled once", texts().count("echo: dup check") == 1, texts().count("echo: dup check"))

sleep_id = inject("do SLEEP now")
wait_for(lambda: any("SLEEP" in c["prompt"] for c in calls()), 30)
time.sleep(25)
typing = [s for s in sent()["statuses"] if s.get("message_id") == sleep_id and s.get("typing_indicator")]
check("the typing indicator is kept alive through a long turn", len(typing) >= 2, len(typing))
inject("/stop")
check("/stop is acknowledged", wait_for(lambda: any(t.startswith("Stopped everything") for t in texts()), 30))
time.sleep(3)
check("the stopped turn sends nothing", "echo: do SLEEP now" not in texts())
check("…and is recorded as stopped",
      q("select status from agent.runs where input like '%do SLEEP now%' order by id desc limit 1") == "stopped")

inject("/new")
check("/new is acknowledged", wait_for(lambda: "Okay, starting a fresh conversation." in texts(), 30))
inject("after reset")
wait_for(lambda: "echo: after reset" in texts(), 30)
last = [c for c in calls() if c["prompt"].endswith("after reset")]
check("the next message starts a fresh session", last and last[-1]["resume"] is None, last)

print("--- the worker sprite")
inject("please DELEGATE a report")
check("the orchestrator replies straight after delegating", wait_for(lambda: "echo: please DELEGATE a report" in texts(), 40))
check("the job is queued in agent.runs and started",
      wait_for(lambda: q("select status from agent.runs where kind = 'worker' order by id limit 1") in ("running", "finished"), 30))
check("the worker's report reaches the orchestrator, then the owner",
      wait_for(lambda: [t for t in texts() if t.startswith("echo: Its step-by-step log: /data/worker/")], 150, 2), texts()[-3:])
worker = q("select concat_ws('|', id, label, status, attempts, coalesce(jsonb_array_length(steps), 0), usage->>'output_tokens') "
           "from agent.runs where kind = 'worker' order by id limit 1").split("|")
check("provisioning failed once and the job was retried", worker[2:4] == ["finished", "2"], worker)
check("the worker run keeps its step log and usage", int(worker[4]) >= 2 and worker[5] == "5", worker)
run_id, label = worker[0], worker[1]
check("its output and step log are copied here, owned by the agent",
      sh(f"stat -c %U /data/worker/{label}/report.txt /data/worker/{label}/steps.json").split() == ["agent", "agent"],
      sh(f"ls -l /data/worker/{label}"))
reports = [c for c in calls() if c["prompt"].startswith("[Worker report")]
check("the report names the job, run and outcome",
      reports and reports[0]["prompt"].startswith(f"[Worker report · job {label} · run {run_id} · finished]"), reports[:1])
check("…and continues the chat session", reports and reports[0]["resume"])
worker_calls = jsonl("/tmp/worker_calls.log")
w = worker_calls[-1] if worker_calls else {}
check("the worker gets model credentials, AGENT_* vars, a display, MCP config, a timeout and its run id",
      w.get("has_model_token") and w.get("agent_var") == "visible" and w.get("display") == ":99" and w.get("mcp")
      and w.get("timeout") == "60" and w.get("run_id") == run_id, w)
check("the worker's secrets file is deleted once read", sh("test -e /workspace/agent/.env && echo present || echo gone").strip() == "gone")
env_log = sh("cat /tmp/sprite-env.log")
check("the Tasks API holds the sprite awake, then releases it", "PUT /v1/tasks/job-" in env_log and "DELETE /v1/tasks/job-" in env_log, env_log)
check("the sprite is created once", sh("grep -c ' create agent-worker' /tmp/fake_sprites.log").strip() == "1")

inject("DELEGATE TWICE please")
check("two queued jobs both run",
      wait_for(lambda: q("select count(*) from agent.runs where kind = 'worker' and status = 'finished'") == "3", 150, 2),
      q("select id, status, attempts from agent.runs where kind = 'worker'"))
check("…one after the other", q("select count(*) from agent.runs a join agent.runs b on a.id < b.id "
                                "where a.kind = 'worker' and b.kind = 'worker' and b.started_at < a.finished_at") == "0")
launches = sh("grep -c 'exec bash -lc sprite-env curl -X PUT' /tmp/fake_sprites.log").strip()
check("4 launches so far: one retry plus three jobs", launches == "4", launches)

inject("RESIDENTIAL please")
residential = wait_for(lambda: [c for c in jsonl("/tmp/worker_calls.log") if c["brief"].startswith("Residential job.")], 90, 2)
check("a --residential job gets the Byteful keys and an authorization note",
      residential and residential[0]["has_byteful"] and "authorized a residential IP" in residential[0]["brief"], residential)
check("other worker jobs never see the Byteful keys",
      not any(c["has_byteful"] for c in jsonl("/tmp/worker_calls.log") if not c["brief"].startswith("Residential job.")))
check("the orchestrator never sees the Byteful keys", not any(c["has_byteful"] for c in calls()))
wait_for(lambda: q("select count(*) from agent.runs where kind = 'worker' and status in ('queued', 'running')") == "0", 60, 2)

inject("DELEGATE CANCELME now")
wait_for(lambda: q("select count(*) from agent.runs where kind = 'worker' and status = 'running'") == "1", 30)
time.sleep(4)
inject("STATUS check")
check("delegate --status shows the running job and its brief",
      wait_for(lambda: "CANCELME" in sh("cat /data/home/status_seen.txt 2>/dev/null"), 40), sh("cat /data/home/status_seen.txt"))
inject("/stop")
check("/stop cancels the worker job, which still reports",
      wait_for(lambda: [c for c in calls() if c["prompt"].startswith("[Worker report") and "· cancelled]" in c["prompt"]], 90, 2))
check("the worker's processes are gone", wait_for(lambda: not sh("pgrep -f 'sprite-home/.local/bin/claude'").strip(), 30))

print("--- schedules and the message log")
q("insert into agent.schedules (name, cron, prompt) values ('quiet', '* * * * *', 'quiet task')")
q("insert into agent.schedules (name, cron, prompt, precheck) values ('changed', '* * * * *', 'report task', 'echo changed-data')")
q("insert into agent.schedules (name, cron, prompt, precheck) values ('nothing', '* * * * *', 'never runs', 'true')")
q("insert into agent.schedules (name, cron, prompt, run_once) values ('once', '* * * * *', 'one shot', true)")
q("insert into agent.schedules (name, cron, prompt) values ('broken', 'not a cron', 'x')")
check("an invalid cron is disabled with a warning",
      wait_for(lambda: any("“broken” was disabled" in t for t in texts()), 150, 2), q("select name, enabled, last_error from agent.schedules"))
check("precheck output reaches the task's prompt",
      wait_for(lambda: [c for c in calls() if "report task" in c["prompt"] and "changed-data" in c["prompt"]], 150, 2))
check("the task's reply is delivered", wait_for(lambda: "echo: changed-data" in texts(), 60))
check("a quiet task runs", wait_for(lambda: [c for c in calls() if c["prompt"].startswith("[Scheduled task “quiet”")], 60))
check("…replies SILENT, which isn't sent", "SILENT" not in texts())
check("…and is recorded as silent", q("select status from agent.runs where kind = 'task' and label = 'quiet' limit 1") == "silent")
check("an empty precheck skips the model", not [c for c in calls() if "never runs" in c["prompt"]])
check("a run_once schedule disables itself", wait_for(lambda: q("select enabled from agent.schedules where name = 'once'") == "f", 90, 2))
check("inbound messages are logged", q("select count(*) from agent.messages where direction = 'in' and body = 'hello'") == "1")
check("every outbound message was sent", q("select count(*) from agent.messages where direction = 'out' and status <> 'sent'") == "0",
      q("select id, status, error from agent.messages where direction = 'out' and status <> 'sent'"))

print(f"\n{sum(results)}/{len(results)} checks passed")
sys.exit(0 if all(results) else 1)
