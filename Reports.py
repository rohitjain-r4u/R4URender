
# Reports.py — Saved Reports + Graphical Metrics
from flask import Blueprint, render_template, request, session, redirect, url_for, flash, jsonify, send_file
import psycopg2, psycopg2.extras
import io, csv, json, re as _re
from datetime import datetime, date, timedelta
from contextlib import contextmanager
from pagination import Paginator, sanitize_page_params
from AllCandidates import normalize_list_field, _extract_filters_from_mapping

# ---------------------- DB CONFIG ----------------------
import os
DATABASE_URL = os.getenv("DATABASE_URL")


@contextmanager
def db_cursor():
    # Skip DB entirely if disabled
    if os.getenv('DISABLE_DB'):
        from contextlib import contextmanager
        @contextmanager
        def _noop():
            yield (None, None)
        return _noop()
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=5, sslmode='require')
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        yield conn, cur
    finally:
        cur.close()
        conn.close()

reports_bp = Blueprint("reports_bp", __name__, template_folder="templates")

# ---------------------- Helpers ----------------------
def (if not os.getenv('DISABLE_DB'):
    if not os.getenv('DISABLE_DB'):
    _ensure_table() if not os.getenv('DISABLE_DB') else None):
    with db_cursor() as (conn, cur):
        cur.execute("""
        CREATE TABLE IF NOT EXISTS saved_reports (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            slug TEXT UNIQUE NOT NULL,
            filters JSONB NOT NULL,
            is_public BOOLEAN NOT NULL DEFAULT TRUE,
            created_by INTEGER NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
        """); conn.commit()

def _slugify(name):
    base = _re.sub(r'[^a-z0-9]+', '-', (name or '').strip().lower())
    return base.strip('-') or 'report'

def _require_login(): return 'user_id' in session
def _is_admin(): return bool(session.get('is_admin') or session.get('role') == 'admin')

@reports_bp.record_once
def _init(_state):
    (if not os.getenv('DISABLE_DB'):
    if not os.getenv('DISABLE_DB'):
    _ensure_table() if not os.getenv('DISABLE_DB') else None)

# ---------------------- Pages ----------------------
@reports_bp.route("/reports")
def reports_index():
    if not _require_login(): return redirect(url_for("login"))

    page, per_page = sanitize_page_params(request.args.get("page"),request.args.get("per_page"))
    def getp(k, d=""): return (request.args.get(k) or d).strip()
    filt = _extract_filters_from_mapping(getp)
    where_sql, params = filt["where_sql"], filt["params"]

    # Table data
    with db_cursor() as (conn, cur):
        cur.execute(f"SELECT COUNT(*) AS total FROM candidates c LEFT JOIN requirements r ON r.id=c.requirement_id WHERE {where_sql}", params)
        total = cur.fetchone()["total"]
        cur.execute(f"""
            SELECT c.*, r.id AS req_id, r.requirement_name, r.client_name
            FROM candidates c
            LEFT JOIN requirements r ON r.id=c.requirement_id
            WHERE {where_sql}
            ORDER BY c.added_date DESC, c.id DESC
            LIMIT %s OFFSET %s
        """, params + [per_page, (page-1)*per_page])
        candidates = cur.fetchall() or []
        for c in candidates:
            c["phones"] = normalize_list_field(c.get("phones"))
            c["emails"] = normalize_list_field(c.get("emails"))

    paginator = Paginator(total, page, per_page, url_for("reports_bp.reports_index"), request.args.to_dict())

    # Saved reports list
    with db_cursor() as (conn, cur):
        if _is_admin():
            cur.execute("SELECT id,name,slug,is_public,created_by,created_at FROM saved_reports ORDER BY created_at DESC LIMIT 250")
        else:
            cur.execute("""SELECT id,name,slug,is_public,created_by,created_at
                           FROM saved_reports WHERE is_public=TRUE OR created_by=%s
                           ORDER BY created_at DESC LIMIT 250""", [session.get("user_id")])
        saved = cur.fetchall() or []

        # Fetch requirement options for dropdown
        try:
            cur.execute("SELECT id, requirement_name FROM requirements ORDER BY requirement_name ASC")
            requirement_options = cur.fetchall() or []
        except Exception:
            requirement_options = []
    


    return render_template("reports.html",
        candidates=candidates, paginator=paginator, total=total,
        requirement_options=requirement_options,
        **filt["filters"], can_save=_is_admin(), saved_reports=saved
    )

# ---------------------- JSON: Metrics for Charts ----------------------
@reports_bp.route("/reports/metrics.json")
def metrics_json():
    if not _require_login(): return jsonify(ok=False, error="unauthorized"), 403
    def gp(k, d=""): return (request.args.get(k) or d).strip()
    f = _extract_filters_from_mapping(gp)
    where_sql, params = f["where_sql"], f["params"]

    today = date.today()
    last_30 = today - timedelta(days=30)

    data = {}
    with db_cursor() as (conn, cur):
        # KPIs
        cur.execute(f"SELECT COUNT(*) AS total FROM candidates c LEFT JOIN requirements r ON r.id=c.requirement_id WHERE {where_sql}", params)
        data['total_candidates'] = int(cur.fetchone()['total'])

        cur.execute(f"""SELECT COUNT(*) AS cnt FROM candidates c
                        LEFT JOIN requirements r ON r.id=c.requirement_id
                        WHERE {where_sql} AND (c.added_date::date) >= %s""", params+[last_30])
        data['new_30d'] = int(cur.fetchone()['cnt'])

        cur.execute(f"""SELECT COUNT(*) AS cnt FROM candidates c
                        LEFT JOIN requirements r ON r.id=c.requirement_id
                        WHERE {where_sql} AND (c.interview_date::date) = %s""", params+[today])
        data['interviews_today'] = int(cur.fetchone()['cnt'])

        cur.execute(f"""SELECT COUNT(*) AS cnt FROM candidates c
                        LEFT JOIN requirements r ON r.id=c.requirement_id
                        WHERE {where_sql} AND (c.interview_date::date) = %s""", params+[today + timedelta(days=1)])
        data['interviews_tomorrow'] = int(cur.fetchone()['cnt'])

        # By calling status
        cur.execute(f"""SELECT COALESCE(c.calling_status,'(none)') AS label, COUNT(*) AS cnt
                        FROM candidates c LEFT JOIN requirements r ON r.id=c.requirement_id
                        WHERE {where_sql}
                        GROUP BY label ORDER BY cnt DESC LIMIT 10""", params)
        data['by_calling_status'] = cur.fetchall()

        # By profile status
        cur.execute(f"""SELECT COALESCE(c.profile_status,'(none)') AS label, COUNT(*) AS cnt
                        FROM candidates c LEFT JOIN requirements r ON r.id=c.requirement_id
                        WHERE {where_sql}
                        GROUP BY label ORDER BY cnt DESC LIMIT 10""", params)
        data['by_profile_status'] = cur.fetchall()

        # Top requirements
        cur.execute(f"""SELECT r.requirement_name AS label, COUNT(*) AS cnt
                        FROM candidates c LEFT JOIN requirements r ON r.id=c.requirement_id
                        WHERE {where_sql}
                        GROUP BY r.requirement_name
                        ORDER BY cnt DESC NULLS LAST LIMIT 10""", params)
        data['top_requirements'] = cur.fetchall()

        # Added per week (last 12 weeks)
        cur.execute(f"""SELECT to_char(date_trunc('week', c.added_date), 'YYYY-MM-DD') AS week, COUNT(*) AS cnt
                        FROM candidates c LEFT JOIN requirements r ON r.id=c.requirement_id
                        WHERE {where_sql} AND c.added_date >= NOW() - INTERVAL '84 days'
                        GROUP BY week ORDER BY week""", params)
        data['added_per_week'] = cur.fetchall()

    return jsonify(ok=True, data=data)



# ---------------------- CRUD JSON ----------------------
@reports_bp.route("/reports/save", methods=["POST"])
def save_report():
    if not (_require_login() and _is_admin()):
        return jsonify(ok=False, error="unauthorized"), 403
    payload = request.get_json() or {}
    name = (payload.get("name") or "").strip()
    filters = payload.get("filters") or request.args.to_dict()
    is_public = bool(payload.get("is_public", True))
    if not name: return jsonify(ok=False, error="Name required"), 400
    slug = _slugify(name)

    with db_cursor() as (conn, cur):
        i = 2
        while True:
            cur.execute("SELECT 1 FROM saved_reports WHERE slug=%s", [slug])
            if not cur.fetchone(): break
            slug = f"{_slugify(name)}-{i}"; i += 1
        cur.execute("""INSERT INTO saved_reports (name,slug,filters,is_public,created_by) 
                       VALUES (%s,%s,%s::jsonb,%s,%s) RETURNING id,slug""",
                    [name, slug, json.dumps(filters), is_public, session["user_id"]])
        row = cur.fetchone(); conn.commit()
    return jsonify(ok=True, id=row["id"], slug=row["slug"], run_url=url_for("reports_bp.run_saved_report", slug=row["slug"]))

@reports_bp.route("/reports/list")
def list_reports():
    if not _require_login(): return jsonify(ok=False, error="unauthorized"), 403
    with db_cursor() as (conn, cur):
        if _is_admin():
            cur.execute("SELECT * FROM saved_reports ORDER BY created_at DESC LIMIT 500")
        else:
            cur.execute("SELECT * FROM saved_reports WHERE is_public=TRUE OR created_by=%s ORDER BY created_at DESC LIMIT 500", [session["user_id"]])
        return jsonify(ok=True, items=cur.fetchall())

@reports_bp.route("/reports/delete/<int:rid>", methods=["POST"])
def delete_report(rid):
    if not (_require_login() and _is_admin()):
        return jsonify(ok=False, error="unauthorized"), 403
    with db_cursor() as (conn, cur):
        cur.execute("DELETE FROM saved_reports WHERE id=%s", [rid])
        conn.commit()
    return jsonify(ok=True)

@reports_bp.route("/reports/r/<slug>")
def run_saved_report(slug):
    if not _require_login(): return redirect(url_for("login"))
    with db_cursor() as (conn, cur):
        cur.execute("SELECT * FROM saved_reports WHERE slug=%s", [slug])
        row = cur.fetchone()
        if not row:
            flash("Report not found","warning"); return redirect(url_for("reports_bp.reports_index"))
        if not row["is_public"] and not (_is_admin() or row["created_by"]==session["user_id"]):
            flash("Access denied","danger"); return redirect(url_for("reports_bp.reports_index"))
        return redirect(url_for("reports_bp.reports_index", **row["filters"]))

@reports_bp.route("/reports/export_all", methods=["POST"])
def export_all():
    if not _require_login(): return redirect(url_for("login"))
    payload = request.get_json() or {}
    def gp(k,d=""): return (payload.get(k) or d).strip()
    filt = _extract_filters_from_mapping(gp)
    where_sql, params = filt["where_sql"], filt["params"]

    with db_cursor() as (conn, cur):
        cur.execute(f"""
            SELECT c.*, r.id AS req_id, r.requirement_name, r.client_name
            FROM candidates c
            LEFT JOIN requirements r ON r.id=c.requirement_id
            WHERE {where_sql} ORDER BY c.added_date DESC, c.id DESC
        """, params)
        rows = cur.fetchall()

    headers = ["Job Title","Requirement","Candidate Name","Total Experience",
               "Phone Number","Email ID","Notice Period","Current Location",
               "Calling Status","Profile Status","Comments","Added By"]

    sio = io.StringIO(); writer = csv.writer(sio); writer.writerow(headers)
    for c in rows:
        phones = ", ".join(normalize_list_field(c.get("phones")))
        emails = ", ".join(normalize_list_field(c.get("emails")))
        req = (c.get("requirement_name") or f"Requirement {c.get('req_id')}") if c.get("req_id") else "-"
        if c.get("client_name"): req += f" ({c['client_name']})"
        writer.writerow([
            c.get("job_title") or "-", req, c.get("candidate_name") or "-",
            c.get("total_experience") or "-", phones or "-", emails or "-",
            c.get("notice_period") or "-", c.get("current_location") or "-",
            c.get("calling_status") or "-", c.get("profile_status") or "-",
            c.get("comments") or "-", c.get("added_by_name") or c.get("added_by") or "—"
        ])
    mem = io.BytesIO(sio.getvalue().encode("utf-8-sig")); mem.seek(0)
    return send_file(mem, as_attachment=True, download_name="report_export.csv", mimetype="text/csv")
