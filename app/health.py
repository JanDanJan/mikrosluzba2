import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from app.db import SessionLocal, OutboxEvent

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        """
        Serve the `/healthz` endpoint with outbox backlog information.

        I return 200 with a JSON body `{"status":"ok","outbox_unsent": <int>}`
        on success. For any exception, I return 500 with a JSON error.

        @return None
        """
        if self.path != "/healthz":
            self.send_response(404); self.end_headers(); return
        try:
            with SessionLocal() as s:
                unsent = s.query(OutboxEvent).filter_by(sent=False).count()
            body = json.dumps({"status":"ok", "outbox_unsent": unsent}).encode()
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except Exception as e:
            body = json.dumps({"status":"error","detail":str(e)}).encode()
            self.send_response(500)
            self.send_header("Content-Type","application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

def start_health_server(stop_event, host="0.0.0.0", port=8080):
    """
    Start a background HTTP server exposing `/healthz` until stopped.

    @param stop_event: `threading.Event`-like; when set, the server shuts down.
    @param host: Interface to bind (default `0.0.0.0`).
    @param port: TCP port to listen on (default 8080).
    @return None
    """
    httpd = HTTPServer((host, port), Handler)
    t = Thread(target=httpd.serve_forever, daemon=True)
    t.start()

    def _shutdown():
        httpd.shutdown()
    stop_event.wait()
    _shutdown()
