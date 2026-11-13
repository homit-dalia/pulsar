# The orchestrator machine: the bridge plus Claude Code. No browser here; that lives on the
# worker sprite (worker/provision.sh).
FROM python:3.12-slim-bookworm

ARG CLAUDE_CODE_VERSION=2.1.311

ENV DEBIAN_FRONTEND=noninteractive \
    UV_PYTHON_DOWNLOADS=never

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl git jq procps postgresql-client tzdata \
 && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.12.23 /uv /uvx /usr/local/bin/

# Claude Code, pinned. DISABLE_UPDATES in fly.toml stops it updating itself.
RUN curl -fsSL https://claude.ai/install.sh -o /tmp/install-claude.sh \
 && HOME=/opt/claude bash /tmp/install-claude.sh "$CLAUDE_CODE_VERSION" \
 && ln -s /opt/claude/.local/bin/claude /usr/local/bin/claude \
 && chmod -R a+rX /opt/claude \
 && rm /tmp/install-claude.sh

# boto3 for the orchestrator agent's own scripts (object storage); the bridge has its own venv.
RUN pip install --no-cache-dir "boto3==1.40.*"

# The bridge's two dependencies.
RUN uv venv /opt/bridge --python /usr/local/bin/python3 \
 && uv pip install --python /opt/bridge/bin/python "psycopg[binary]==3.3.9" "croniter==7.0.0"

RUN useradd --create-home --uid 1000 --shell /bin/bash agent
COPY bridge.py /app/bridge.py
COPY agent/ /app/agent/
COPY worker/ /app/worker/
COPY supabase/tables.sql /app/supabase/tables.sql
COPY --chmod=755 notify delegate /usr/local/bin/

CMD ["/opt/bridge/bin/python", "/app/bridge.py"]
