"""A minimal OpenAI-compatible chat endpoint for failure experiments (F8). It answers at once with a text that
cites no numbers and names no feature, so the explainer's faithfulness gate accepts it. Killing this
container is the "LLM went down" fault."""
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ANSWER = "Several signals in this payment point to elevated risk, so it needs review."


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        body = json.dumps({"choices": [{"message": {"role": "assistant", "content": ANSWER}}]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
