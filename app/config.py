"""
Config loader (optional). Kept minimal to avoid breaking existing behavior.
You can migrate secrets to env vars over time and read them here.
"""
import os
from dotenv import load_dotenv

# Load local .env if present
load_dotenv()

class Config:
    SECRET_KEY = os.environ.get("SECRET_KEY", "changeme")
    DB_NAME = os.environ.get("DB_NAME")
    DB_USER = os.environ.get("DB_USER")
    DB_PASS = os.environ.get("DB_PASS")
    DB_HOST = os.environ.get("DB_HOST")
