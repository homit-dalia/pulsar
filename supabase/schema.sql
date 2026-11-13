-- Run once in the Supabase SQL editor. Creates a login role that can only use its own
-- schema "agent". The agent creates any other tables it needs there itself.

create role agent_rw with login password 'REPLACE-WITH-A-LONG-RANDOM-PASSWORD';
grant agent_rw to postgres;  -- lets this session create objects owned by agent_rw
create schema agent authorization agent_rw;

set role agent_rw;

-- Bridge bookkeeping (WhatsApp poll offset, owner id).
create table agent.bridge_state (
  key        text primary key,
  value      text not null,
  updated_at timestamptz not null default now()
);

-- Schedules the agent writes for itself; the poller fires them.
create table agent.schedules (
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

reset role;
