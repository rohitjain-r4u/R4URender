"""
App entrypoint for Gunicorn and Flask CLI.
"""

# Import the Flask app directly from main.py
from main import app

# Expose for WSGI servers like Gunicorn
application = app
