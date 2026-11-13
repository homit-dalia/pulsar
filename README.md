# Personal agent

You talk to an **orchestrator**: Claude Code running headless on DeepSeek, on one small, always-on Fly Machine, reachable over WhatsApp. When a job is heavy, long, or needs a browser, it delegates to **one worker**: a Fly Sprite with its own Claude and a stealth Chrome, which sleeps (and costs nothing for compute) when idle. The worker reports back, and the orchestrator decides whether you need to hear about it.

The repo is infrastructure only. What the agents do, and how, they work out and save for themselves: skills, scripts, notes, and Postgres tables.

```
you ⇄ WhatsApp ⇄ api.whatsapp.com/agent/v1 (long-poll)
                    │
orchestrator   Fly Machine, shared-cpu-1x 512MB, always on (~$4/mo)
  bridge.py ── polls WhatsApp · fires agent.schedules · runs `claude -p` one job at a time
  claude    ── chat, light web lookups, scheduling, deciding what to tell you
     │ delegate "brief"            ▲ [Worker report …] + output files
     ▼                             │
worker         Fly Sprite "agent-worker" (created on first use, sleeps when idle)
  run.sh ── provisions once (Chrome, Xvfb, stealth MCP, pinned Claude Code) · runs `claude -p` on the brief
Supabase       agent.messages (every message, in and out) · agent.runs (every turn and job)
               agent.events (what the agents did) · agent.schedules · plus any tables the agents create
Storage        a private Supabase Storage bucket for files the agents keep (resumes, screenshots)
```

Outbound messages are never sent directly. Both a reply and a `notify` become a `pending` row in `agent.messages`, and one sender thread delivers them in order, about once a second.

| File | Purpose |
|---|---|
| `fly.toml`, `Dockerfile` | The orchestrator machine (no browser): Claude Code 2.1.311 pinned, model settings |
| `bridge.py` | The only real code: WhatsApp transport, schedule clock, job queue, worker sprite lifecycle |
| `agent/CLAUDE.md` | Tells the orchestrator about its environment and the worker. No task instructions |
| `worker/` | Copied into the sprite per job: its `CLAUDE.md`, MCP config, `provision.sh`, `run.sh`, `status.py` |
| `notify`, `delegate` | Commands for the orchestrator: queue a message to you; hand a job to the worker |
| `supabase/schema.sql` | One-time setup: a scoped role and its `agent` schema |
| `supabase/tables.sql` | The infrastructure's tables. The bridge applies them on every boot, so new ones ship with a deploy |

## Setup

1. **Supabase.** Create a free project. In the SQL editor, run `supabase/schema.sql` after replacing the password; the bridge creates the tables itself. For the connection string, use the URI from **Connect** with the user changed to `agent_rw` and that password. A pooler URI has a `.<ref>` suffix on the user, which you keep.
2. **WhatsApp.** Settings → Agents → Create an agent → open its chat → Chat info → API key.
3. **DeepSeek.** Create an API key and add some prepaid credit.
4. **Sprites.** Create an API token for your Sprites organization at [sprites.dev](https://sprites.dev).
5. **Fly and GitHub.** Run this once, from the repo directory. `fly.toml` names the app; change the name there if it's taken.

```sh
fly auth login
fly apps create agent-orchestrator-r2ui
fly volumes create agent_data --region iad --size 1 --yes
fly secrets set \
  ANTHROPIC_AUTH_TOKEN='<DeepSeek API key>' \
  WA_AGENT_TOKEN='<WhatsApp agent API key>' \
  SPRITES_TOKEN='<Sprites API token>' \
  DATABASE_URL='postgresql://agent_rw.<ref>:<password>@<pooler-host>:5432/postgres'
fly tokens create deploy | gh secret set FLY_API_TOKEN   # the token goes straight into GitHub
```

6. **Push to `main`.** `.github/workflows/deploy.yml` deploys every push to `main`, skipping pushes that only change the README or `supabase/schema.sql`. You can also run it from the Actions tab.
7. Send your agent a WhatsApp message. The first time the orchestrator delegates, the worker sprite is created and set up, which takes a few extra minutes.

Optional: give the agents a mailbox, for example a new Gmail account with 2-Step Verification and an app password:

```sh
fly secrets set AGENT_EMAIL_ADDRESS='…' AGENT_EMAIL_PASSWORD='<app password>' \
  AGENT_IMAP_HOST=imap.gmail.com AGENT_SMTP_HOST=smtp.gmail.com
```

Optional, but the agents' instructions expect it: file storage in a Supabase Storage bucket over its S3 protocol.
1. In Storage, create a **private** bucket named `agent`.
2. Under Storage settings → S3 access keys, create a key.
3. Copy the endpoint and region from the same page, then:

```sh
fly secrets set AGENT_S3_ENDPOINT='https://<ref>.supabase.co/storage/v1/s3' AGENT_S3_REGION='<region>' AGENT_S3_BUCKET=agent \
  AGENT_S3_ACCESS_KEY_ID='…' AGENT_S3_SECRET_ACCESS_KEY='…'
```

Optional: a paid residential-IP proxy (Byteful). These keys deliberately have no `AGENT_` prefix, so neither agent sees them by default. The bridge hands them to a single worker job only when the orchestrator delegates it with `--residential`, and the orchestrator is told to do that only when you explicitly ask:

```sh
fly secrets set BYTEFUL_PUBLIC_KEY='…' BYTEFUL_PRIVATE_KEY='…'
```

Optional: set `HEALTHCHECK_URL` (for example a free healthchecks.io check with a 10-minute period). The bridge pings it every 5 minutes, so you'll know if the orchestrator stops.

## Using it

- Talk to it in WhatsApp. `/stop` stops everything: the orchestrator's current job, its queue, and the worker's job. `/new` starts a fresh conversation, which also happens automatically after 6 hours of silence.
- To give the agents a credential: `fly secrets set AGENT_SOMETHING=... -a <app>`, then tell the orchestrator the variable name. Both agents get `AGENT_*` variables. Never paste secrets into the chat.
- Logs: `fly logs -a <app>`. Shell: `fly ssh console -a <app>`. The worker: `sprite console agent-worker`, or look under `/workspace/agent/jobs/`.
- To change the model or effort level, edit `[env]` in `fly.toml` and push. Those settings apply to both agents.
- A deploy restarts the orchestrator. A turn that was in progress is re-run after the restart, and a worker job carries on in the sprite.
- To see what happened, look in Supabase (Table Editor → schema `agent`):
  - `messages` has the conversation.
  - `runs` has every orchestrator turn and worker job, with its input, output and status.
  - `events` has what the agents logged along the way.
  - Any task-specific tables the agents created are in the same schema.

## Costs

- **Orchestrator.** A 512MB machine running 24/7 is about $4/month. A 1GB volume is $0.15/month. Fly waives personal bills under $5/month.
- **Worker sprite.** Billed only while it runs, and on what it actually uses: $0.0385 per CPU-hour and $0.021875 per GB-hour of memory. A browser job averaging half a CPU and 1.5GB comes to about 5¢/hour. Its disk costs about $0.02/GB-month while it sleeps.
- **DeepSeek.** Billed per token from your prepaid balance, so the balance is your spending cap. Off-peak is half price, and US daytime is off-peak.
- **Supabase.** Free tier. The bridge queries it every minute, which keeps the project from pausing.

## Things to know

- WhatsApp agent chats aren't end-to-end encrypted. Reinstalling WhatsApp invalidates the key; set `WA_AGENT_TOKEN` again.
- Only one process can poll a WhatsApp key. Don't run `bridge.py` locally while the deployed one is running.
- Anthropic doesn't support Claude Code with non-Claude models. The version is pinned in `Dockerfile` and `worker/provision.sh`, so test before changing it.
- The bridge holds the Sprites token and only ever touches the one sprite named in `SPRITE_NAME`. The agents never see the token, and jobs run one at a time.
- The worker's model key and `AGENT_*` credentials are written into the sprite for each job and deleted as soon as the job starts.
- `mcp-stealth-chrome` (0.11.0) is a young project with one maintainer, and it has access to the worker's browser profile. It's pinned; review it before upgrading.
- Sprites have datacenter IPs. Some sites challenge or block them however stealthy the browser is.
