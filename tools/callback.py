"""Demo callback receiver; paths /ok, /flaky and /fail control responses."""

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import ClassVar


class Callback(BaseHTTPRequestHandler):
    events: ClassVar[list] = []

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.events.append({"path": self.path, "body": body})
        count = sum(event["body"]["event_id"] == body["event_id"] for event in self.events)
        status = 500 if self.path == "/fail" or (self.path == "/flaky" and count == 1) else 200
        self.send_response(status)
        self.end_headers()

    def do_GET(self):
        body = json.dumps(self.events).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8080), Callback).serve_forever()
