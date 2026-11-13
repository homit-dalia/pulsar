"""Fake Sprites API for the end-to-end tests: the test container itself plays the sprite.

Implements what bridge.py calls: GET/POST /v1/sprites, PUT fs/write, GET fs/read, POST exec.
"""
import json
import subprocess
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

PORT = int(sys.argv[1])
SPRITES = set()
LOG = Path("/tmp/fake_sprites.log")
SPRITE_ENV = {"HOME": "/sprite-home", "PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8"}


def log(line):
    with LOG.open("a") as f:
        f.write(f"{time.strftime('%H:%M:%S')} {line}\n")


class Handler(BaseHTTPRequestHandler):
    def reply(self, code, body=b"", content_type="application/json"):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def route(self):
        if self.headers.get("Authorization") != "Bearer sprites-test-token":
            self.reply(401, b'{"error":"unauthorized"}')
            return None, None
        url = urlparse(self.path)
        return url.path.strip("/").split("/"), parse_qs(url.query)

    def do_GET(self):
        parts, query = self.route()
        if parts is None:
            return
        if len(parts) == 3 and parts[1] == "sprites":
            return self.reply(200, json.dumps({"name": parts[2]}).encode()) if parts[2] in SPRITES else self.reply(404, b"{}")
        if parts[-2:] == ["fs", "read"]:
            path = Path(query["path"][0])
            return self.reply(200, path.read_bytes(), "application/octet-stream") if path.is_file() else self.reply(404, b"{}")
        self.reply(404, b"{}")

    def do_POST(self):
        parts, query = self.route()
        if parts is None:
            return
        body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        if parts == ["v1", "sprites"]:
            request = json.loads(body)
            SPRITES.add(request["name"])
            log(f"create {request['name']} wait_for_capacity={request.get('wait_for_capacity')}")
            return self.reply(201, json.dumps({"name": request["name"]}).encode())
        if parts[-1] == "exec":
            argv = query["cmd"]
            log("exec " + " ".join(argv)[:300].replace("\n", " "))
            done = subprocess.run(argv, env=SPRITE_ENV, cwd="/", capture_output=True, timeout=120)
            return self.reply(200, done.stdout, "application/octet-stream")
        self.reply(404, b"{}")

    def do_PUT(self):
        parts, query = self.route()
        if parts is None:
            return
        body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        if parts[-2:] == ["fs", "write"]:
            path = Path(query["path"][0])
            if query.get("mkdir") == ["true"]:
                path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
            return self.reply(200, b"{}")
        self.reply(404, b"{}")

    def log_message(self, *_):
        pass


ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
