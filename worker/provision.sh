#!/bin/bash
# One-time setup of the worker sprite (its disk persists). run.sh re-runs this whenever it changes.
set -eux
SUDO=""
[ "$(id -u)" -eq 0 ] || SUDO="sudo -n"
export DEBIAN_FRONTEND=noninteractive
apt() { for i in 1 2 3; do $SUDO apt-get "$@" && return 0; sleep 10; done; return 1; }  # mirrors are flaky

apt update
apt install -y --no-install-recommends ca-certificates curl gnupg jq postgresql-client python3-boto3 \
  xvfb fonts-liberation fonts-noto-color-emoji fonts-noto-core fonts-noto-cjk libgl1 libglib2.0-0

# Real Google Chrome (amd64 only), driven by the stealth MCP server.
if ! command -v google-chrome > /dev/null; then
  curl -fsSL https://dl.google.com/linux/linux_signing_key.pub | $SUDO gpg --dearmor --yes -o /usr/share/keyrings/google-chrome.gpg
  echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] https://dl.google.com/linux/chrome/deb/ stable main" \
    | $SUDO tee /etc/apt/sources.list.d/google-chrome.list > /dev/null
  apt update
  apt install -y --no-install-recommends google-chrome-stable
fi

# Pinned to match the orchestrator image. Sprites ship their own uv; tools go to ~/.local/bin either way.
export PATH="$HOME/.local/bin:$PATH"
command -v uv > /dev/null || curl -LsSf https://astral.sh/uv/0.12.23/install.sh | sh
UV_TOOL_BIN_DIR="$HOME/.local/bin" uv tool install --force "mcp-stealth-chrome==0.11.0"
# The sprite's default python3 is its own pyenv build, not Ubuntu's; give it boto3 as well.
python3 -m pip install --user --quiet "boto3==1.40.*"
# The installer honours DISABLE_UPDATES, which would leave the sprite's preinstalled version in place.
curl -fsSL https://claude.ai/install.sh | env -u DISABLE_UPDATES bash -s 2.1.311
"$HOME/.local/bin/claude" --version
google-chrome --version
