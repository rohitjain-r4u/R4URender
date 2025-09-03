
# AllCandidates.py — merged with Export-All + total count passed to template (FIXED .strip() on None)
from flask import Blueprint, render_template, request, session, redirect, url_for, flash, jsonify, send_file
from pagination import Paginator, sanitize_page_params
import sys
from main import get_db_cursor

import json
import re as _re
import io
import csv


def normalize_list_field(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        items = [str(x).strip() for x in value if str(x).strip()]
    else:
        s = str(value).strip()
        if s.startswith('[') and s.endswith(']'):
            try:
                arr = json.loads(s)
                items = [str(x).strip() for x in arr if str(x).strip()]
            except Exception:
                items = [s] if s else []
        else:
            parts = _re.split(r'[;,|]+', s)
            items = [p.strip() for p in parts if p.strip()]
    cleaned = []
    for it in items:
        if _re.fullmatch(r'\d+\.0', it):
            it = it[:-2]
        cleaned.append(it)
    return cleaned


def _as_list(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(x).strip() for x in value if str(x).strip()]
    if isinstance(value, str):
        s = value.strip()
        if s.startswith('[') and s.endswith(']'):
            try:
                arr = json.loads(s)
                return [str(x).strip() for x in arr if str(x).strip()]
            except Exception:
                pass
        parts = _re.split(r'[;,|]+', s)
        return [str(x).strip() for x in parts if str(x).strip()]
    return [str(value).strip()]


def _strip_trailing_decimal(s):
    if isinstance(s, (int, float)):
        s = str(s)
    s = str(s).strip()
    if _re.fullmatch(r'\d+\.0', s):
        return s[:-2]
    return s


def normalize_phones(value):
    items = _as_list(value)
    out = []
    for it in items:
        it = _strip_trailing_decimal(it)
        out.append(it)
    return out


def normalize_emails(value):
    items = _as_list(value)
    out = [it for it in items if '@' in it]
    return out


all_candidates_bp = Blueprint('all_candidates_bp', __name__, template_folder='templates')


def _helpers():
    return get_db_cursor


def _extract_filters_from_mapping(getter):
    """Build where_sql + params using the same semantics as the list page."""
    name           = getter('name', '')
    phone          = getter('phone', '')
    email          = getter('email', '')
    location       = getter('location', '')
    calling_status = getter('calling_status', '')
    profile_status = getter('profile_status', '')
    requirement_id = getter('requirement_id', '')  # was None -> caused .strip() on None
    interview_date = getter('interview_date', '')
    key_skills_raw = getter('key_skills', '')

    # Added-by-me toggle (default ON) — inserted minimally
    added_by_me = getter('added_by_me', '1').strip().lower()
    is_added_by_me = added_by_me in ('1', 'true', 'on', 'yes')
    _skills_terms = [t.strip() for t in _re.split(r'[;,]+', key_skills_raw) if t.strip()]

    where = ["1=1"]
    params = []
    if name:
        where.append("c.candidate_name ILIKE %s"); params.append(f"%{name}%")
    if phone:
        where.append("COALESCE(c.phones::text,'') ILIKE %s"); params.append(f"%{phone}%")
    if email:
        where.append("COALESCE(c.emails::text,'') ILIKE %s"); params.append(f"%{email}%")
    for _term in _skills_terms:
        where.append("COALESCE(c.key_skills,'') ILIKE %s"); params.append(f"%{_term}%")
    if location:
        where.append("COALESCE(c.current_location,'') ILIKE %s"); params.append(f"%{location}%")
    if calling_status:
        where.append("COALESCE(c.calling_status,'') = %s"); params.append(calling_status)
    if profile_status:
        where.append("COALESCE(c.profile_status,'') = %s"); params.append(profile_status)
    if requirement_id:
        where.append("c.requirement_id = %s"); params.append(requirement_id)
    if interview_date:
        where.append("(c.interview_date::date) = %s"); params.append(interview_date)


    # Apply 'Added by me' filter only when enabled and user_id is in session — inserted minimally
    if is_added_by_me and ('user_id' in session):
        where.append("CAST(c.added_by AS TEXT) = %s")
        params.append(str(session['user_id']))
    return {
        'where_sql': " AND ".join(where),
        'params': params,
        'filters': {
            'name': name, 'phone': phone, 'email': email, 'location': location,
            'calling_status': calling_status, 'profile_status': profile_status,
            'requirement_id': requirement_id, 'interview_date': interview_date,
            'key_skills': key_skills_raw
        }
    }


@all_candidates_bp.route('/candidates', methods=['GET'])
def all_candidates():
    if 'user_id' not in session:
        return redirect(url_for('login'))

    get_db_cursor

    # Consistent pagination
    page, per_page = sanitize_page_params(
        request.args.get('page'),
        request.args.get('per_page'),
        default_per_page=50,
        max_per_page=200
    )

    # Safe getter that always returns a string
    def _safe_get(key, default=''):
        v = request.args.get(key)
        if v is None:
            v = default
        return str(v).strip()

    filt = _extract_filters_from_mapping(_safe_get)
    where_sql = filt['where_sql']
    params = filt['params']

    order_sql = "ORDER BY c.added_date DESC, c.id DESC"

    try:
        with get_db_cursor() as (conn, cur):
            cur.execute(f"""
                SELECT COUNT(*) AS total
                FROM candidates c
                LEFT JOIN requirements r ON r.id = c.requirement_id
                WHERE {where_sql}
            """, params)
            row = cur.fetchone() or {}
            total = row.get('total', 0)

            offset = (page - 1) * per_page
            cur.execute(f"""
                SELECT
                    c.*,
                    r.id   AS req_id,
                    r.requirement_name,
                    r.client_name
                FROM candidates c
                LEFT JOIN requirements r ON r.id = c.requirement_id
                WHERE {where_sql}
                {order_sql}
                LIMIT %s OFFSET %s
            """, params + [per_page, offset])
            candidates = cur.fetchall() or []

            for c in candidates:
                c['phones'] = normalize_list_field(c.get('phones'))
                c['emails'] = normalize_list_field(c.get('emails'))

    except Exception:
        flash('Error loading candidates', 'danger')
        candidates, total = [], 0

    paginator = Paginator(
        total=total,
        page=page,
        per_page=per_page,
        base_url=url_for('all_candidates_bp.all_candidates'),
        args=request.args.to_dict()
    )

    return render_template(
        'all_candidates.html',
        requirement=None,
        candidates=candidates,
        paginator=paginator,
        total=total,
        name=filt['filters']['name'],
        phone=filt['filters']['phone'],
        email=filt['filters']['email'],
        location=filt['filters']['location'],
        calling_status=filt['filters']['calling_status'],
        profile_status=filt['filters']['profile_status'],
        requirement_id=filt['filters']['requirement_id'],
        interview_date=filt['filters']['interview_date'],
        key_skills=filt['filters']['key_skills']
    ,
        added_by_me=filt.get('filters', {}).get('added_by_me', '1')
    )


@all_candidates_bp.route('/candidates/requirements.json', methods=['GET'])
def all_candidates_requirements_json():
    if 'user_id' not in session:
        return jsonify({'ok': False, 'error': 'unauthorized'}), 401

    get_db_cursor
    try:
        with get_db_cursor() as (conn, cur):
            cur.execute("""
                SELECT id, requirement_name, client_name
                FROM requirements
                ORDER BY id DESC
                LIMIT 1000
            """)
            rows = cur.fetchall() or []
            return jsonify({'ok': True, 'requirements': rows})
    except Exception:
        return jsonify({'ok': False, 'error': 'failed'}), 500


@all_candidates_bp.route('/candidates/export_all', methods=['POST'])
def export_all_candidates_csv():
    if 'user_id' not in session:
        return redirect(url_for('login'))

    payload = request.get_json(silent=True) or {}

    def _getp(name, default=''):
        v = payload.get(name)
        if v is None:
            v = default
        return str(v).strip()

    get_db_cursor
    filt = _extract_filters_from_mapping(_getp)
    where_sql = filt['where_sql']
    params = filt['params']
    order_sql = "ORDER BY c.added_date DESC, c.id DESC"

    with get_db_cursor() as (conn, cur):
        cur.execute(f"""
            SELECT
                c.*,
                r.id   AS req_id,
                r.requirement_name,
                r.client_name
            FROM candidates c
            LEFT JOIN requirements r ON r.id = c.requirement_id
            WHERE {where_sql}
            {order_sql}
        """, params)
        rows = cur.fetchall() or []

    headers = [
        "Job Title", "Requirement", "Candidate Name", "Total Experience",
        "Phone Number", "Email ID", "Notice Period", "Current Location",
        "Calling Status", "Profile Status", "Comments", "Added By"
    ]

    sio = io.StringIO()
    writer = csv.writer(sio)
    writer.writerow(headers)
    for c in rows:
        phones = ", ".join(normalize_list_field(c.get('phones')))
        emails = ", ".join(normalize_list_field(c.get('emails')))
        req_label = ""
        if c.get('req_id'):
            req_label = (c.get('requirement_name') or f"Requirement {c.get('req_id')}")
            client_name = c.get('client_name')
            if client_name:
                req_label += f" ({client_name})"
        writer.writerow([
            c.get('job_title') or "-",
            req_label or "-",
            c.get('candidate_name') or "-",
            c.get('total_experience') or "-",
            phones or "-",
            emails or "-",
            c.get('notice_period') or "-",
            c.get('current_location') or "-",
            c.get('calling_status') or "-",
            c.get('profile_status') or "-",
            c.get('comments') or "-",
            c.get('added_by_name') or c.get('added_by') or "—",
        ])

    mem = io.BytesIO(sio.getvalue().encode('utf-8-sig'))
    mem.seek(0)
    return send_file(
        mem,
        as_attachment=True,
        download_name="candidates_export.csv",
        mimetype="text/csv"
    )