import sys
import os
import traceback
import logging

logger = logging.getLogger("oxys.api")

# Ensure project root is first in sys.path
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

os.environ["VERCEL"] = "1"

try:
    from server.main import app
except Exception as init_err:
    from fastapi import FastAPI
    from fastapi.responses import HTMLResponse
    app = FastAPI(title="OXYS Diagnostic Error")
    err_tb = traceback.format_exc()
    
    @app.api_route("/{full_path:path}", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "PATCH"])
    async def error_route(full_path: str = ""):
        return HTMLResponse(
            status_code=500,
            content=f"""
            <html>
            <body style="background:#0d1117;color:#f85149;font-family:monospace;padding:32px;">
                <h1>OXYS Initialization Error on Vercel</h1>
                <p><b>Error:</b> {init_err}</p>
                <pre style="background:#161b22;padding:16px;border-radius:8px;color:#c9d1d9;">{err_tb}</pre>
            </body>
            </html>
            """
        )

try:
    from mangum import Mangum
    handler = Mangum(app, lifespan="off")
except Exception:
    handler = app
