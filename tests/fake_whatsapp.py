"""Fake WhatsApp Agent Platform API for the end-to-end tests.

Test helpers: POST /_inject (a list of inbound message objects) and GET /_sent.
A text containing RETRYME is refused once with a 503, to exercise the sender's retries.
"""
import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

PORT = int(sys.argv[1])
ENTRIES, SENT, UPLOADS, STATUSES, FAILED_ONCE = [], [], [], [], set()
COND = threading.Condition()


class Handler(BaseHTTPRequestHandler):
    def reply(self, code, obj=None):
        self.send_response(code)
        if obj is None:
            self.end_headers()
            return
        data = json.dumps(obj).encode()
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def authed(self):
        if self.headers.get("Authorization") != "Bearer wa-test-token":
            self.reply(401, {"error": {"code": 190}})
            return False
        return True

    def do_GET(self):
        url = urlparse(self.path)
        query = parse_qs(url.query)
        if url.path == "/_sent":
            return self.reply(200, {"sent": SENT, "uploads": UPLOADS, "statuses": STATUSES})
        if not self.authed():
            return
        if url.path.startswith("/content/"):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"%PDF-1.4 fake resume")
            return
        if url.path == "/agent/v1/updates":
            offset = int(query.get("offset", ["0"])[0])
            deadline = time.time() + min(int(query.get("timeout", ["15"])[0]), 3)  # short polls keep tests fast
            with COND:
                while len(ENTRIES) <= offset and time.time() < deadline:
                    COND.wait(max(0.0, deadline - time.time()))
                batch = ENTRIES[offset : offset + int(query.get("limit", ["50"])[0])]
            if not batch:
                return self.reply(204)
            value = {"messaging_product": "whatsapp", "contacts": [], "messages": batch, "statuses": []}
            return self.reply(200, {"object": "whatsapp_agent_platform", "next_offset": offset + len(batch),
                                    "entry": [{"id": "1", "changes": [{"field": "messages", "value": value}]}]})
        if url.path.startswith("/agent/v1/media/"):
            media_id = url.path.rsplit("/", 1)[1]
            return self.reply(200, {"url": f"http://127.0.0.1:{PORT}/content/{media_id}", "id": media_id,
                                    "mime_type": "application/pdf", "messaging_product": "whatsapp"})
        self.reply(404)

    def do_POST(self):
        url = urlparse(self.path)
        raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        if url.path == "/_inject":
            with COND:
                ENTRIES.extend(json.loads(raw))
                COND.notify_all()
            return self.reply(200, {"ok": True})
        if not self.authed():
            return
        if url.path == "/agent/v1/messages":
            body = json.loads(raw)
            text = body.get("text", {}).get("body", "")
            if "RETRYME" in text and text not in FAILED_ONCE:
                FAILED_ONCE.add(text)
                return self.reply(503, {"error": {"code": 131016}})
            if not body.get("to", "").startswith("user:"):
                return self.reply(400, {"error": {"code": 131009}})
            SENT.append(body)
            return self.reply(200, {"messaging_product": "whatsapp", "messages": [{"id": f"wamid.out{len(SENT)}"}],
                                    "contacts": [{"input": body["to"], "wa_id": body["to"]}]})
        if url.path == "/agent/v1/statuses":
            STATUSES.append({"at": time.time(), **json.loads(raw)})
            return self.reply(200, {"success": True})
        if url.path == "/agent/v1/media":
            UPLOADS.append({"bytes": len(raw)})
            return self.reply(200, {"id": f"media-up-{len(UPLOADS)}"})
        self.reply(404)

    def log_message(self, *_):
        pass


ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
