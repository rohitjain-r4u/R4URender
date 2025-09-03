"""
Domain blueprint: misc
Extracted from main blueprint without changing logic.
"""
from flask import Blueprint
from ..core import *  # import original helpers and app context
bp = Blueprint("misc", __name__)

@bp.route('/api/csrf-token', methods=['GET'])
def api_csrf_token():

    token = get_csrf_token() or issue_csrf_token()
    return api_ok({'csrf_token': token})


# PATCH 1 APPLIED
from flask import Flask, render_template, request, redirect, url_for, jsonify, flash, abort
from datetime import date, datetime
import pandas as pd
import io
import json
from werkzeug.utils import secure_filename

app = Flask(__name__)
# --- CSRF HARDENING (11) ---
# If you already use Flask-WTF elsewhere, reuse that instance.
try:
    from flask_wtf import CSRFProtect
    csrf = CSRFProtect(app)
    app.config.setdefault("WTF_CSRF_ENABLED", True)
except Exception:
    csrf = None  # app still works, but enable if lib is available

# Helper to include token in templates (if using Flask-WTF)
@app.context_processor
def inject_csrf_token():
    token = None
    try:
        from flask_wtf.csrf import generate_csrf
        token = generate_csrf()
    except Exception:
        pass
    return dict(csrf_token=token)

# -------------------------------------------------------------------
# (1) FULL-PAGE IMPORT WIZARD
# -------------------------------------------------------------------
