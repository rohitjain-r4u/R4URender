# myteam.py
import os
from contextlib import contextmanager
from datetime import datetime
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras
from flask import (
    Blueprint, render_template, request, redirect, url_for, flash, session, abort,
    jsonify
)

# Blueprint
myteam_bp = Blueprint(
    "myteam",
    __name__,
    template_folder="templates",
    static_folder="static",
)

# ---------- DB connection helper (matches dashboard_routes.py style) ----------
def build_dsn_from_env():
    # Prefer DATABASE_URL if provided
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return db_url

    # Otherwise build from individual env vars (optional)
    user = os.getenv("DB_USER") or os.getenv("PGUSER") or "postgres"
    password = os.getenv("DB_PASSWORD") or os.getenv("PGPASSWORD") or ""
    host = os.getenv("DB_HOST") or "localhost"
    port = os.getenv("DB_PORT") or "5432"
    dbname = os.getenv("DB_NAME") or os.getenv("PGDATABASE") or "job_portal"

    # Return a dict of connection params for psycopg2.connect(**params)
    return {
        "user": user,
        "password": password,
        "host": host,
        "port": port,
        "dbname": dbname
    }

@contextmanager
def get_db_cursor():
    """
    Yields (conn, cur) where cur is RealDictCursor (rows as dicts).
    If DATABASE_URL exists, psycopg2.connect(DATABASE_URL) is used.
    Else psycopg2.connect(**kwargs) is used when DB parts are in env.
    Commits on success, rolls back on exception.
    """
    conn = None
    cur = None
    dsn = build_dsn_from_env()
    try:
        if isinstance(dsn, str):
            conn = psycopg2.connect(dsn)
        else:
            # dsn is dict of params
            conn = psycopg2.connect(**dsn)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        yield conn, cur
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# ---------- Helpers ----------
def fmtdate(value):
    if not value:
        return ""
    if isinstance(value, (str,)):
        return value
    try:
        return value.strftime("%B %d, %Y")
    except Exception:
        return str(value)

def is_absolute_url(s: str) -> bool:
    if not s:
        return False
    try:
        p = urlparse(s)
        return bool(p.scheme) and bool(p.netloc)
    except Exception:
        return False

def get_image_url(v):
    if not v:
        return ("data:image/svg+xml;utf8,"
                "<svg xmlns='http://www.w3.org/2000/svg' width='160' height='160'>"
                "<rect width='100%' height='100%' fill='%23e2e8f0'/>"
                "<text x='50%' y='50%' fill='%236b7280' font-size='36' text-anchor='middle' dominant-baseline='central'>?</text>"
                "</svg>")
    if is_absolute_url(v):
        return v
    vv = v.replace("\\", "/").strip()
    if '/static/' in vv:
        return vv if vv.startswith('/') else '/' + vv
    return url_for('static', filename=f"uploads/{vv}")

# Register helpers as template filters on the blueprint so Jinja can find them
# (this fixes errors like "No filter named 'fmtdate'")
myteam_bp.add_app_template_filter(fmtdate, name='fmtdate')
myteam_bp.add_app_template_filter(get_image_url, name='get_image_url')

# ---- safe_url helper (add this block into myteam.py) ----
from flask import url_for
from werkzeug.routing import BuildError

def safe_url(name, **kwargs):
    """
    Try blueprint-qualified endpoint first (myteam.name), then bare name.
    Returns '#' if neither exists.
    Usage in templates: {{ safe_url('admin_teams') }}
    """
    # if user passed 'myteam.xyz' explicitly, try that as-is first
    candidates = [name] if '.' in name else [f"myteam.{name}", name]
    for ep in candidates:
        try:
            return url_for(ep, **kwargs)
        except BuildError:
            continue
    return '#'

# inject into the blueprint template context (so templates can call safe_url directly)
@myteam_bp.app_context_processor
def _inject_helpers():
    return {"safe_url": safe_url}
# ---- end safe_url block ----

# ---------------- get_image_url helper (paste into myteam.py) ----------------
from urllib.parse import urlparse
from flask import url_for

def _is_absolute_url(s: str) -> bool:
    if not s:
        return False
    try:
        p = urlparse(s)
        return bool(p.scheme) and bool(p.netloc)
    except Exception:
        return False

def get_image_url(db_value: str) -> str:
    """
    Convert stored image value into a usable URL for <img src="...">.
    - If value is an absolute URL (http/https) -> return unchanged.
    - If value contains '/static/' or starts with 'static/' -> ensure leading '/' (Flask static path).
    - If value is a plain filename (e.g. 'yashasv.jpg') -> treat as 'static/uploads/<filename>'.
    - If None/empty -> return a small inline SVG placeholder data URL.
    """
    if not db_value:
        # simple SVG placeholder (small, no external requests)
        placeholder = ("data:image/svg+xml;utf8,"
                       "<svg xmlns='http://www.w3.org/2000/svg' width='160' height='160'>"
                       "<rect width='100%' height='100%' fill='%23e6eef8'/>"
                       "<text x='50%' y='50%' fill='%23454657' font-size='36' text-anchor='middle' dominant-baseline='central'>?</text>"
                       "</svg>")
        return placeholder

    v = str(db_value).strip().replace("\\", "/")

    # absolute URL => return as-is
    if _is_absolute_url(v):
        return v

    # If user stored a path that already contains /static/, return it (ensure leading slash)
    if '/static/' in v:
        return v if v.startswith('/') else '/' + v

    # If user stored 'static/uploads/xxx' or 'uploads/xxx', normalize to url_for('static', ...)
    if v.startswith('static/'):
        # remove leading 'static/' and hand to url_for
        rel = v[len('static/'):]
        return url_for('static', filename=rel)
    if v.startswith('uploads/'):
        return url_for('static', filename=v[len(''):] )  # uploads/xxx -> static/uploads/xxx

    # Otherwise treat as filename under static/uploads
    return url_for('static', filename=f"uploads/{v}")

# inject helper into template context for the blueprint
@myteam_bp.app_context_processor
def _inject_myteam_helpers():
    return {"get_image_url": get_image_url}
# ---------------------------------------------------------------------------



# ---------- Routes ----------
@myteam_bp.route('/teams')
def teams():
    show_all = request.args.get('show_all', '0') == '1'
    if show_all:
        q = "SELECT * FROM my_teams ORDER BY name"
        params = ()
    else:
        q = "SELECT * FROM my_teams WHERE is_active = true ORDER BY name"
        params = ()
    with get_db_cursor() as (conn, cur):
        cur.execute(q, params)
        rows = cur.fetchall()
    # decorate rows for template convenience
    for r in rows:
        r['joining_date_fmt'] = fmtdate(r.get('joining_date'))
        r['birthday_fmt'] = fmtdate(r.get('birthday'))
        r['anniversary_fmt'] = fmtdate(r.get('anniversary'))
        r['image_url_resolved'] = get_image_url(r.get('image_url'))
    role = session.get('role', 'employee')
    return render_template('teams.html', members=rows, role=role, show_all=show_all)

# ----- Admin pages -----
def require_admin():
    if session.get('role') != 'admin':
        abort(403)

@myteam_bp.route('/admin/teams')
def admin_teams():
    require_admin()
    with get_db_cursor() as (conn, cur):
        cur.execute("SELECT * FROM my_teams ORDER BY name")
        rows = cur.fetchall()
    return render_template('admin_list.html', members=rows)

@myteam_bp.route('/admin/teams/add', methods=['GET','POST'])
def admin_add():
    require_admin()
    if request.method == 'POST':
        name = request.form.get('name','').strip()
        username = request.form.get('username') or None
        email = request.form.get('email') or None
        designation = request.form.get('designation') or None
        image_url = request.form.get('image_url') or None
        joining_date = request.form.get('joining_date') or None
        birthday = request.form.get('birthday') or None
        anniversary = request.form.get('anniversary') or None
        city = request.form.get('city') or None
        address = request.form.get('address') or None
        phone_number = request.form.get('phone_number') or None

        if not name:
            flash("Name required", "danger")
            return redirect(request.url)

        with get_db_cursor() as (conn, cur):
            cur.execute("""
                INSERT INTO my_teams
                (name, username, email, joining_date, birthday, anniversary, designation, image_url, city, address, phone_number, is_active, created_at, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, true, now(), now())
                RETURNING id
            """, (name, username, email, joining_date or None, birthday or None, anniversary or None,
                  designation, image_url, city, address, phone_number))
            new_id = cur.fetchone()['id']
        flash("Member added", "success")
        return redirect(url_for('myteam.admin_teams'))
    return render_template('admin_add.html')

@myteam_bp.route('/admin/teams/<int:member_id>/edit', methods=['GET','POST'])
def admin_edit(member_id):
    require_admin()
    if request.method == 'POST':
        name = request.form.get('name','').strip()
        username = request.form.get('username') or None
        email = request.form.get('email') or None
        designation = request.form.get('designation') or None
        image_url = request.form.get('image_url') or None
        joining_date = request.form.get('joining_date') or None
        birthday = request.form.get('birthday') or None
        anniversary = request.form.get('anniversary') or None
        city = request.form.get('city') or None
        address = request.form.get('address') or None
        phone_number = request.form.get('phone_number') or None

        with get_db_cursor() as (conn, cur):
            cur.execute("""
                UPDATE my_teams SET
                  name=%s, username=%s, email=%s, joining_date=%s, birthday=%s, anniversary=%s,
                  designation=%s, image_url=%s, city=%s, address=%s, phone_number=%s, updated_at=now()
                WHERE id=%s
            """, (name, username, email, joining_date or None, birthday or None, anniversary or None,
                  designation, image_url, city, address, phone_number, member_id))
        flash("Member updated", "success")
        return redirect(url_for('myteam.admin_teams'))

    with get_db_cursor() as (conn, cur):
        cur.execute("SELECT * FROM my_teams WHERE id = %s", (member_id,))
        m = cur.fetchone()
    if not m:
        abort(404)
    # decorate for template
    m['joining_date_fmt'] = fmtdate(m.get('joining_date'))
    m['birthday_fmt'] = fmtdate(m.get('birthday'))
    m['anniversary_fmt'] = fmtdate(m.get('anniversary'))
    m['image_url_resolved'] = get_image_url(m.get('image_url'))
    return render_template('admin_edit.html', m=m)

@myteam_bp.route('/admin/teams/<int:member_id>/toggle', methods=['POST'])
def admin_toggle(member_id):
    require_admin()
    with get_db_cursor() as (conn, cur):
        cur.execute("UPDATE my_teams SET is_active = NOT is_active, updated_at = now() WHERE id = %s", (member_id,))
    flash("Toggled active", "info")
    return redirect(request.referrer or url_for('myteam.admin_teams'))

@myteam_bp.route('/admin/teams/<int:member_id>/delete', methods=['POST'])
def admin_delete(member_id):
    require_admin()
    with get_db_cursor() as (conn, cur):
        cur.execute("DELETE FROM my_teams WHERE id = %s", (member_id,))
    flash("Member deleted", "warning")
    return redirect(url_for('myteam.admin_teams'))

# ---------- Simple API endpoints ----------
@myteam_bp.route('/api/teams', methods=['POST'])
def api_create_member():
    data = request.get_json() or {}
    def pd(v):
        return v if v else None
    with get_db_cursor() as (conn, cur):
        cur.execute("""
            INSERT INTO my_teams (name, username, email, joining_date, birthday, anniversary, designation, image_url, city, address, phone_number, is_active, created_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now(), now())
            RETURNING id
        """, (pd(data.get('name','Unnamed')), pd(data.get('username')), pd(data.get('email')),
              pd(data.get('joining_date')), pd(data.get('birthday')), pd(data.get('anniversary')),
              pd(data.get('designation')), pd(data.get('image_url')), pd(data.get('city')), pd(data.get('address')),
              pd(data.get('phone_number')), bool(data.get('is_active', True))))
        new_id = cur.fetchone()['id']
    return jsonify({"message":"created","id": new_id}), 201

@myteam_bp.route('/api/teams/<int:member_id>', methods=['PUT'])
def api_update_member(member_id):
    data = request.get_json() or {}
    with get_db_cursor() as (conn, cur):
        cur.execute("""
            UPDATE my_teams SET
              name=%s, username=%s, email=%s, joining_date=%s, birthday=%s, anniversary=%s,
              designation=%s, image_url=%s, city=%s, address=%s, phone_number=%s, is_active=%s, updated_at=now()
            WHERE id=%s
        """, (
            data.get('name'), data.get('username'), data.get('email'),
            data.get('joining_date'), data.get('birthday'), data.get('anniversary'),
            data.get('designation'), data.get('image_url'), data.get('city'), data.get('address'),
            data.get('phone_number'), bool(data.get('is_active', True)), member_id
        ))
    return jsonify({"message":"updated","id": member_id})

# done
