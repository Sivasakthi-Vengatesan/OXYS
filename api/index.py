import sys
import os

# Add parent directory to path so server module can be imported
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from server.main import app
