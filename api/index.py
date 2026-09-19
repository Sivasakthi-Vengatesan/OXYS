import sys
import os
import traceback

# Add root directory to sys.path
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

os.environ["VERCEL"] = "1"

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

# Top-level FastAPI instance required by Vercel CLI static analyzer
app = FastAPI(
    title="OXYS API",
    description="OXYS - Real-time integrity protection for streaming data.",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

try:
    from server.main import app as backend_app
    app.mount("/", backend_app)
except Exception as e:
    err_trace = traceback.format_exc()

    @app.api_route("/{full_path:path}", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "PATCH"])
    async def fallback_catchall(full_path: str = ""):
        return HTMLResponse(
            status_code=500,
            content=f"""<!DOCTYPE html>
<html>
<head><title>OXYS Serverless Diagnostic</title></head>
<body style="background:#0d1117;color:#f85149;font-family:monospace;padding:32px;line-height:1.5;">
    <h2>OXYS Serverless Boot Exception</h2>
    <p><b>Error:</b> {e}</p>
    <pre style="background:#161b22;color:#e6edf3;padding:16px;border-radius:8px;overflow:auto;border:1px solid #30363d;">{err_trace}</pre>
</body>
</html>"""
        )


