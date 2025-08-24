import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from app.db import SessionLocal, OutboxEvent

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
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
    httpd = HTTPServer((host, port), Handler)
    t = Thread(target=httpd.serve_forever, daemon=True)
    t.start()

    def _shutdown():
        httpd.shutdown()
    stop_event.wait()
    _shutdown()
