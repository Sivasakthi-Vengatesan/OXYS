import sys
import os
from fastapi import FastAPI

app = FastAPI()

@app.get("/api/test")
def test_endpoint():
    return {
        "status": "OK",
        "python_version": sys.version,
        "platform": sys.platform
    }
