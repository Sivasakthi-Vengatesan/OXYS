import sys
import os
import traceback

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

os.environ["VERCEL"] = "1"

try:
    from server.main import app
except Exception as e:
    err_tb = traceback.format_exc()
    from fastapi import FastAPI
    from fastapi.responses import HTMLResponse
    app = FastAPI(title="OXYS Diagnostic Error")
    
    @app.api_route("/{full_path:path}", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "PATCH"])
    async def catch_all(full_path: str = ""):
        return HTMLResponse(
            status_code=200,
            content=f"""
            <html>
            <head><title>OXYS Vercel Diagnostic</title></head>
            <body style="background:#0d1117;color:#f85149;font-family:monospace;padding:32px;">
                <h1 style="color:#58a6ff;">OXYS Startup Diagnostic</h1>
                <p><b>Exception:</b> {e}</p>
                <pre style="background:#161b22;padding:16px;border-radius:8px;color:#c9d1d9;overflow-x:auto;">{err_tb}</pre>
            </body>
            </html>
            """
        )
