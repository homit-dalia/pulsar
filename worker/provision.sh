#!/bin/bash
# One-time setup of the worker sprite (its disk persists). run.sh re-runs this whenever it changes.
set -eux
SUDO=""
[ "$(id -u)" -eq 0 ] || SUDO="sudo -n"
export DEBIAN_FRONTEND=noninteractive

$SUDO apt-get update
$SUDO apt-get install -y --no-install-recommends ca-certificates curl gnupg jq postgresql-client \
  xvfb fonts-liberation fonts-noto-color-emoji libgl1 libglib2.0-0

# Real Google Chrome (amd64 only), driven by the stealth MCP server.
if ! command -v google-chrome > /dev/null; then
  curl -fsSL https://dl.google.com/linux/linux_signing_key.pub | $SUDO gpg --dearmor --yes -o /usr/share/keyrings/google-chrome.gpg
  echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] https://dl.google.com/linux/chrome/deb/ stable main" \
    | $SUDO tee /etc/apt/sources.list.d/google-chrome.list > /dev/null
  $SUDO apt-get update
  $SUDO apt-get install -y --no-install-recommends google-chrome-stable
fi

# Pinned to match the orchestrator image.
command -v uv > /dev/null || curl -LsSf https://astral.sh/uv/0.12.23/install.sh | sh
"$HOME/.local/bin/uv" tool install --force "mcp-stealth-chrome==0.11.0"
curl -fsSL https://claude.ai/install.sh | bash -s 2.1.311
"$HOME/.local/bin/claude" --version
google-chrome --version
