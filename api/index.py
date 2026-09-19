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
except Exception as e:
    err_tb = traceback.format_exc()
    logger.error(f"Failed to load OXYS server.main app: {e}\n{err_tb}")
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse
    app = FastAPI(title="OXYS API (Fallback Mode)")
    
    @app.api_route("/{full_path:path}", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "PATCH"])
    async def fallback_handler(full_path: str):
        return JSONResponse(
            status_code=500,
            content={
                "status": "ERROR",
                "message": "OXYS API Backend failed during initialization",
                "error": str(e),
                "traceback": err_tb
            }
        )
