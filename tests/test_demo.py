"""Run the demo script against a local HTTP fixture without external traffic."""
import json
import os
from pathlib import Path
import signal
import subprocess
import threading
import unittest
import uuid
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = Path(__file__).resolve().parents[1]
COMPANY = "00000000-0000-0000-0000-000000000001"


class DemoScriptTests(unittest.TestCase):
    def test_login_and_event_ingestion_preserve_optional_video(self):
        for video in (None, ""):
            with self.subTest(video=video):
                self.check_ingestion(video)

    def check_ingestion(self, video):
        requests = []
        received = threading.Event()

        class Endpoint(BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                requests.append((self.path, dict(self.headers), body))
                login = self.path == "/api/auth/login"
                response = {"access_token": "fixture-token"} if login else {
                    "count": 1, "first_sequence": 1, "last_sequence": 1,
                }
                self.send_response(200 if login else 201)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(response).encode())
                if not login:
                    received.set()

            def log_message(self, *_):
                pass

        with ThreadingHTTPServer(("127.0.0.1", 0), Endpoint) as server:
            serving = threading.Thread(target=server.serve_forever, daemon=True)
            serving.start()
            environment = dict(os.environ, API_BASE=f"http://127.0.0.1:{server.server_port}",
                               COMPANY_ID=COMPANY, CONTROL_EMAIL="fixture@example.test",
                               CONTROL_PASSWORD="fixture-password")
            environment.pop("VIDEO_URL", None)
            if video is not None:
                environment["VIDEO_URL"] = video
            process = subprocess.Popen(
                ["bash", str(ROOT / "scripts/add-events.sh")], env=environment,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, start_new_session=True,
            )
            try:
                self.assertTrue(received.wait(15), "demo script did not ingest an event")
            finally:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                output, errors = process.communicate(timeout=5)
                server.shutdown()
                serving.join(timeout=5)

        self.assertEqual(errors, "", output)
        self.assertEqual(requests[0][2], {
            "email": "fixture@example.test", "password": "fixture-password",
        })
        path, headers, payload = requests[1]
        self.assertEqual(path, f"/api/v1/companies/{COMPANY}/events/ingest")
        self.assertEqual(headers["Authorization"], "Bearer fixture-token")
        uuid.UUID(headers["Idempotency-Key"])
        event, = payload["events"]
        self.assertTrue(event["id"].startswith("LAN-DEMO-"))
        self.assertEqual(event["event_type_id"], "person_detected")
        self.assertEqual(event["preview_url"], "https://placehold.co/640x360.jpg?text=" + event["id"])
        self.assertIsNotNone(datetime.fromisoformat(event["occurred_at"]).tzinfo)
        expected_video = None if video == "" else (
            "https://interactive-examples.mdn.mozilla.net/media/cc0-videos/flower.mp4"
        )
        self.assertEqual(event["video_url"], expected_video)


if __name__ == "__main__":
    unittest.main()
