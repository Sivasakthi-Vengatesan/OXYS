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

from server.main import app

try:
    from mangum import Mangum
    handler = Mangum(app, lifespan="off")
except Exception:
    handler = app
