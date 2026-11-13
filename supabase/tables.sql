-- Tables the bridge and the agents rely on. The bridge runs this file on every boot, and every
-- statement is idempotent, so new tables ship with a deploy. The agents add their own tables to
-- schema "agent" as they need them.

-- Bridge bookkeeping (WhatsApp poll offset, owner id).
create table if not exists agent.bridge_state (
  key        text primary key,
  value      text not null,
  updated_at timestamptz not null default now()
);

-- Schedules the agent writes for itself; the bridge fires them.
create table if not exists agent.schedules (
  id          bigint generated always as identity primary key,
  name        text not null unique,
  cron        text not null,                             -- 5-field cron, e.g. '0 9 * * 1-5'
  tz          text not null default 'America/New_York',
  prompt      text not null,
  precheck    text,                                      -- optional shell command; no output + exit 0 skips the run
  run_once    boolean not null default false,
  enabled     boolean not null default true,
  next_run_at timestamptz,
  last_run_at timestamptz,
  last_error  text,
  created_at  timestamptz not null default now()
);

-- One row per orchestrator turn and per worker job, written by the bridge.
create table if not exists agent.runs (
  id          bigint generated always as identity primary key,
  kind        text not null,                             -- message | task | report | worker
  label       text,                                      -- schedule name or worker job id
  input       text,                                      -- the prompt or brief
  output      text,                                      -- the final reply or the worker's report
  status      text not null default 'running',
  session_id  text,
  started_at  timestamptz not null default now(),
  finished_at timestamptz
);

-- Every message between the owner and the agent. Outbound rows are queued as 'pending' and the
-- bridge's sender delivers them in order, about once a second.
create table if not exists agent.messages (
  id            bigint generated always as identity primary key,
  direction     text not null check (direction in ('in', 'out')),
  body          text not null default '',
  file_path     text,                                    -- outbound attachment, a path on the orchestrator
  status        text not null default 'pending',         -- in: received; out: pending -> sent | failed
  wa_message_id text unique,
  run_id        bigint references agent.runs (id) on delete set null,
  error         text,
  created_at    timestamptz not null default now(),
  sent_at       timestamptz
);
create index if not exists messages_outbox on agent.messages (id) where direction = 'out' and status = 'pending';

-- Columns added after the first release (each statement is a no-op once applied).
alter table agent.runs add column if not exists steps jsonb;                         -- compact log of what the run did
alter table agent.runs add column if not exists usage jsonb;                         -- token counts reported by Claude Code
alter table agent.runs add column if not exists meta jsonb not null default '{}';    -- e.g. transcript path, worker dir, residential
alter table agent.runs add column if not exists attempts int not null default 0;
alter table agent.runs add column if not exists progress text;                       -- worker jobs: the latest step
alter table agent.messages add column if not exists attempts int not null default 0;
alter table agent.messages add column if not exists next_attempt_at timestamptz;
-- Worker jobs are queued as runs: queued -> running (-> cancel_requested) -> finished | failed | cancelled | ...
create index if not exists runs_worker_queue on agent.runs (id)
  where kind = 'worker' and status in ('queued', 'running', 'cancel_requested');

-- What the agents did, in their own words: pages visited, form fields filled, files stored, decisions.
create table if not exists agent.events (
  id         bigint generated always as identity primary key,
  run_id     bigint references agent.runs (id) on delete set null,
  kind       text not null,
  data       jsonb not null default '{}',
  created_at timestamptz not null default now()
);
create index if not exists events_kind on agent.events (kind, created_at);
create index if not exists events_run on agent.events (run_id);
