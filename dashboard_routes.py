
# dashboard_routes.py
# Dashboard blueprint with extended KPIs and drilldowns + admin-only shared layout.
# Routes:
#   GET  /dashboard_data_plus        → JSON for new KPIs & charts
#   GET  /dashboard_drilldown        → HTML table for fullscreen modal
#   GET  /dashboard_layout           → JSON {"layout":[...]} current shared layout
#   POST /dashboard_layout           → (ADMIN ONLY) update shared layout for everyone
#   POST /dashboard_query            → ad-hoc grouped counts for Custom Chart Builder
#   (compat) GET /dashboard_data     → returns same as /dashboard_data_plus

from flask import Blueprint, session, jsonify, request, url_for, current_app
from flask import Blueprint, session, request 
from contextlib import contextmanager
import psycopg2
import psycopg2.extras
import logging
import json
import os
import pathlib

dashboard_bp = Blueprint("dashboard_bp", __name__)

# --- Local DB helper (separate from Req_App to avoid circular imports) ---
logger = logging.getLogger(__name__)

import os
DATABASE_URL = os.getenv("DATABASE_URL")


@contextmanager
def get_db_cursor():
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        yield conn, cur
    except Exception:
        logger.exception("DB connection or query error")
        raise
    finally:
        if cur:
            try: cur.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


# ========= Shared Layout storage (admin-controlled, global for all users) =========
# Reworked to be reliable on Windows: write to Flask instance folder by default,
# or to DASHBOARD_LAYOUT_FILE if provided. Clearer errors on POST.
DEFAULT_WIDGETS = [
    'kpi_total_candidates','kpi_new_candidates_30','kpi_r2_select',
    'kpi_interviews_today','kpi_interviews_tomorrow','kpi_total_requirements', 
    'kpi_combined_offer_pipeline', 'kpi_r3_rejected', 'kpi_recent_fbp',
    'chart_status','chart_reqs_per_recruiter','chart_cands_per_recruiter',
    'chart_profiles_today','chart_profiles_yesterday'
]
ALLOWED_WIDGET_IDS = set(DEFAULT_WIDGETS)

# ===== Shared layout location =====
ENV_PATH = os.environ.get("DASHBOARD_LAYOUT_FILE")

def _layout_path():
    """Return absolute path for the shared layout file."""
    if ENV_PATH:
        p = pathlib.Path(ENV_PATH).expanduser()
        p.parent.mkdir(parents=True, exist_ok=True)
        return p
    # default → instance folder
    current_app.instance_path  # ensures app context exists
    p = pathlib.Path(current_app.instance_path) / "dashboard_layout.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

def read_layout():
    try:
        p = _layout_path()
        if not p.exists():
            return DEFAULT_WIDGETS
        data = json.loads(p.read_text(encoding="utf-8"))
        layout = data.get("layout")
        if isinstance(layout, list):
            return [w for w in layout if w in ALLOWED_WIDGET_IDS]
    except Exception:
        logger.exception("Failed to read layout file")
    return DEFAULT_WIDGETS

def write_layout(layout_list):
    try:
        layout = [w for w in layout_list if w in ALLOWED_WIDGET_IDS]
        p = _layout_path()
        p.write_text(json.dumps({"layout": layout}, ensure_ascii=False, indent=2), encoding="utf-8")
        return True
    except Exception:
        logger.exception("Failed to write layout file")
        return False

@dashboard_bp.route("/dashboard_layout", methods=["GET", "POST"])
def dashboard_layout():
    if request.method == "GET":
        return jsonify({"layout": read_layout()})

    if session.get("role") != "admin":
        return jsonify({"error": "forbidden"}), 403

    body = request.get_json(silent=True) or {}
    layout = body.get("layout")
    if not isinstance(layout, list):
        return jsonify({"error": "layout must be a list"}), 400
    ok = write_layout(layout)
    if not ok:
        return jsonify({"error": "write_failed", "hint": f"Check path: {_layout_path()}"}), 500
    return jsonify({"ok": True, "layout": read_layout()})


# --- JSON for dashboard (requirements + candidates) ---
@dashboard_bp.route('/dashboard_data_plus')
def dashboard_data_plus():
    if 'user_id' not in session:
        return jsonify({'error': 'unauthenticated'}), 401
    try:
        with get_db_cursor() as (conn, cur):
            # Requirements
            cur.execute("SELECT COUNT(*) as total FROM requirements")
            total_req = (cur.fetchone() or {}).get('total', 0)

            cur.execute("SELECT status, COUNT(*) as cnt FROM requirements GROUP BY status")
            status_counts = cur.fetchall() or []

            cur.execute("SELECT assigned_to AS username, COUNT(*) as cnt FROM requirements GROUP BY assigned_to")
            per_recruiter_req = cur.fetchall() or []

            # Candidates (respect recruiter permission)
            me = (session.get('username') or '').strip()
            is_recruiter = (session.get('role') == 'recruiter')

            base_where = ""
            base_params = []
            if is_recruiter and me:
                base_where = " WHERE r.assigned_to ILIKE %s "
                base_params = [f"%{me}%"]

            # KPIs
            cur.execute(f"""
                SELECT COUNT(*) AS total
                FROM candidates c
                LEFT JOIN requirements r ON r.id = c.requirement_id
                {base_where}
            """, tuple(base_params))
            cand_total = (cur.fetchone() or {}).get('total', 0)

            cur.execute(f"""
                SELECT COUNT(*) AS total
                FROM candidates c
                LEFT JOIN requirements r ON r.id = c.requirement_id
                {base_where + (' AND ' if base_where else ' WHERE ')} c.added_date >= CURRENT_DATE - INTERVAL '30 days'
            """, tuple(base_params))
            cand_new_30 = (cur.fetchone() or {}).get('total', 0)

            cur.execute(f"""
                SELECT COUNT(*) AS total
                FROM candidates c
                LEFT JOIN requirements r ON r.id = c.requirement_id
                {base_where + (' AND ' if base_where else ' WHERE ')} c.interview_date::date = CURRENT_DATE
            """, tuple(base_params))
            interviews_today = (cur.fetchone() or {}).get('total', 0)

            cur.execute(f"""
                SELECT COUNT(*) AS total
                FROM candidates c
                LEFT JOIN requirements r ON r.id = c.requirement_id
                {base_where + (' AND ' if base_where else ' WHERE ')} c.interview_date::date = CURRENT_DATE + INTERVAL '1 day'
            """, tuple(base_params))
            interviews_tomorrow = (cur.fetchone() or {}).get('total', 0)

            cur.execute(f"""
                SELECT COUNT(*) AS total
                FROM candidates c
                LEFT JOIN requirements r ON r.id = c.requirement_id
                {base_where + (' AND ' if base_where else ' WHERE ')} c.profile_status = 'R2 Select'
            """, tuple(base_params))
            r2_select_total = (cur.fetchone() or {}).get('total', 0)

            # Candidates per recruiter (all-time)
            cur.execute(f"""
                SELECT COALESCE(NULLIF(TRIM(c.added_by), ''), 'Unknown') AS username, COUNT(*) AS cnt
                FROM candidates c
                LEFT JOIN requirements r ON r.id = c.requirement_id
                {base_where}
                GROUP BY COALESCE(NULLIF(TRIM(c.added_by), ''), 'Unknown')
            """, tuple(base_params))
            per_recruiter_cand = cur.fetchall() or []

            # Today per recruiter
            cur.execute(f"""
                SELECT COALESCE(NULLIF(TRIM(c.added_by), ''), 'Unknown') AS username, COUNT(*) AS cnt
                FROM candidates c
                LEFT JOIN requirements r ON r.id = c.requirement_id
                {base_where + (' AND ' if base_where else ' WHERE ')} c.added_date::date = CURRENT_DATE
                GROUP BY COALESCE(NULLIF(TRIM(c.added_by), ''), 'Unknown')
            """, tuple(base_params))
            per_recruiter_cand_today = cur.fetchall() or []

            # Yesterday per recruiter
            cur.execute(f"""
                SELECT COALESCE(NULLIF(TRIM(c.added_by), ''), 'Unknown') AS username, COUNT(*) AS cnt
                FROM candidates c
                LEFT JOIN requirements r ON r.id = c.requirement_id
                {base_where + (' AND ' if base_where else ' WHERE ')} c.added_date::date = CURRENT_DATE - INTERVAL '1 day'
                GROUP BY COALESCE(NULLIF(TRIM(c.added_by), ''), 'Unknown')
            """, tuple(base_params))
            per_recruiter_cand_yesterday = cur.fetchall() or []

            return jsonify({
                'total_requirements': total_req,
                'status_counts': [dict(r) for r in status_counts],
                'per_recruiter_requirements': [dict(r) for r in per_recruiter_req],
                'cand_total': cand_total,
                'cand_new_30': cand_new_30,
                'interviews_today': interviews_today,
                'interviews_tomorrow': interviews_tomorrow,
                'r2_select_total': r2_select_total,
                'per_recruiter_candidates': [dict(r) for r in per_recruiter_cand],
                'per_recruiter_candidates_today': [dict(r) for r in per_recruiter_cand_today],
                'per_recruiter_candidates_yesterday': [dict(r) for r in per_recruiter_cand_yesterday],
            })
    except Exception:
        logger.exception("Error preparing extended dashboard data")
        return jsonify({'error': 'server error'}), 500


# --- HTML drilldown for fullscreen modal ---
@dashboard_bp.route('/dashboard_drilldown')
def dashboard_drilldown():
    if 'user_id' not in session:
        return "<div class='p-3'>Unauthenticated</div>", 401

    scope = (request.args.get('scope') or '').strip()
    recruiter = (request.args.get('recruiter') or '').strip()
    try:
        limit = max(50, min(int(request.args.get('limit', 500)), 1000))
    except Exception:
        limit = 500

    me = (session.get('username') or '').strip()
    is_recruiter = (session.get('role') == 'recruiter')

    try:
        with get_db_cursor() as (conn, cur):
            where = []
            params = []

            if is_recruiter and me:
                where.append("r.assigned_to ILIKE %s")
                params.append(f"%{me}%")

            if scope == 'total_candidates':
                pass
            elif scope == 'new_candidates_30':
                where.append("c.added_date >= CURRENT_DATE - INTERVAL '30 days'")
            elif scope == 'interviews_today':
                where.append("c.interview_date::date = CURRENT_DATE")
            elif scope == 'interviews_tomorrow':
                where.append("c.interview_date::date = CURRENT_DATE + INTERVAL '1 day'")
            elif scope == 'r2_select':
                where.append("c.profile_status = 'R2 Select'")
            elif scope == 'combined_offer_pipeline':
                where.append("c.profile_status IN ('R3 FBP', 'HR Round', 'R3 Scheduled', 'Offered')")
            elif scope == 'r3_rejected':
                where.append("c.profile_status = 'R3 Rejected'")
            elif scope == 'recent_fbp':
              where.append("c.profile_status IN ('R1 FBP', 'R2 FBP' , 'R3 FBP') AND c.added_date >= CURRENT_DATE - INTERVAL '3 days'")
            elif scope == 'by_recruiter' and recruiter:
                where.append("COALESCE(NULLIF(TRIM(c.added_by), ''), 'Unknown') = %s")
                params.append(recruiter)
            elif scope == 'by_recruiter_today' and recruiter:
                where.append("COALESCE(NULLIF(TRIM(c.added_by), ''), 'Unknown') = %s")
                params.append(recruiter)
                where.append("c.added_date::date = CURRENT_DATE")
            elif scope == 'by_recruiter_yesterday' and recruiter:
                where.append("COALESCE(NULLIF(TRIM(c.added_by), ''), 'Unknown') = %s")
                params.append(recruiter)
                where.append("c.added_date::date = CURRENT_DATE - INTERVAL '1 day'")
            else:
                return "<div class='p-3'>Unsupported scope.</div>", 400

            sql = f"""
                SELECT
                c.id,
                c.candidate_name,
                                c.job_title,
c.emails,
                c.phones,
                c.current_company,
                c.current_location, c.profile_status, c.calling_status,
                  c.interview_date, c.interview_time, c.added_by, c.added_date,
                  r.client_name, r.requirement_name
                FROM candidates c
                LEFT JOIN requirements r ON r.id = c.requirement_id
                {('WHERE ' + ' AND '.join(where)) if where else ''}
                ORDER BY COALESCE(c.added_date, NOW()) DESC
                LIMIT %s
            """
            params.append(limit)
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []

            def esc(v):
                try:
                    return '' if v is None else str(v)
                except Exception:
                    return ''

            html = []
            html.append("""
            <div class="table-responsive">
              <table class="table table-bordered table-hover align-middle">
                <thead class="table-light">
                  <tr>
                    <th>ID</th>
                    <th>Candidate</th>
                    <th>Job Title</th>
                    <th>Company</th>
                    <th>Location</th>
                    <th>Profile Status</th>
                    <th>Calling Status</th>
                    <th>Interview</th>
                    <th>Requirement</th>
                    <th>Client</th>
                    <th>Added By</th>
                    <th>Added On</th>
                    <th>Open</th>
                  </tr>
                </thead>
                <tbody>
            """)
            for r in rows:
                interview = ''
                if r.get('interview_date'):
                    interview = esc(r.get('interview_date'))
                    if r.get('interview_time'):
                        interview += f" {esc(r.get('interview_time'))}"
                url = url_for('view_candidate', cand_id=r['id'])
                html.append(f"""
                  <tr>
                    <td>{esc(r.get('id'))}</td>
                    <td>{esc(r.get('candidate_name'))}</td>
                    <td>{esc(r.get('job_title'))}</td>
                    <td>{esc(r.get('current_company'))}</td>
                    <td>{esc(r.get('current_location'))}</td>
                    <td><span class="badge bg-primary">{esc(r.get('profile_status')) or '-'}</span></td>
                    <td>{esc(r.get('calling_status')) or '-'}</td>
                    <td>{interview or '-'}</td>
                    <td>{esc(r.get('requirement_name'))}</td>
                    <td>{esc(r.get('client_name'))}</td>
                    <td>{esc(r.get('added_by')) or '-'}</td>
                    <td>{esc(r.get('added_date')) or '-'}</td>
                    <td><a class="btn btn-sm btn-outline-primary" href="{url}" target="_blank">View</a></td>
                  </tr>
                """)
            html.append("""
                </tbody>
              </table>
            </div>
            """)
            return "".join(html)
    except Exception:
        logger.exception("Error building drilldown (scope=%s)", scope)
        return "<div class='p-3 text-danger'>Server error while loading data.</div>", 500


# --- Custom Chart Builder backend ---
@dashboard_bp.route('/dashboard_query', methods=['POST'])
def dashboard_query():
    if 'user_id' not in session:
        return jsonify({'error':'unauthenticated'}), 401

    body = request.get_json(silent=True) or {}
    dataset   = body.get('dataset')            # 'candidates' | 'requirements'
    group_by  = body.get('group_by')           # whitelist fields below
    date_from = body.get('date_from')          # 'YYYY-MM-DD' or None
    date_to   = body.get('date_to')
    filters   = body.get('filters') or {}      # dict of extra filters

    if dataset not in ('candidates','requirements'):
        return jsonify({'error':'bad dataset'}), 400

    # Whitelists (avoid SQL injection)
    if dataset == 'candidates':
        table = 'candidates c LEFT JOIN requirements r ON r.id=c.requirement_id'
        group_map = {
            'profile_status': "COALESCE(NULLIF(c.profile_status,''), 'Unknown')",
            'calling_status': "COALESCE(NULLIF(c.calling_status,''), 'Unknown')",
            'added_by':      "COALESCE(NULLIF(TRIM(c.added_by),''), 'Unknown')",
            'client_name':   "COALESCE(NULLIF(r.client_name,''), 'Unknown')",
            'month':         "TO_CHAR(c.added_date::date, 'YYYY-MM')"
        }
        date_col = 'c.added_date'
    else:
        table = 'requirements r'
        group_map = {
            'status':       "COALESCE(NULLIF(r.status,''), 'Unknown')",
            'assigned_to':  "COALESCE(NULLIF(r.assigned_to,''), 'Unassigned')",
            'client_name':  "COALESCE(NULLIF(r.client_name,''), 'Unknown')",
            'month':        "TO_CHAR(r.created_at::date, 'YYYY-MM')"
        }
        date_col = 'r.created_at'

    gexpr = group_map.get(group_by)
    if not gexpr:
        return jsonify({'error':'bad group_by'}), 400

    where = []
    params = []

    # Optional recruiter scoping for recruiters
    me = (session.get('username') or '').strip()
    is_rec = (session.get('role') == 'recruiter')
    if is_rec and dataset == 'candidates':
        where.append("r.assigned_to ILIKE %s")
        params.append(f"%{me}%")

    # Date range
    if date_from:
        where.append(f"{date_col}::date >= %s"); params.append(date_from)
    if date_to:
        where.append(f"{date_col}::date <= %s"); params.append(date_to)

    # Extra filters (whitelist)
    f = {k:(v if v not in (None,'') else None) for k,v in filters.items()}
    if dataset == 'candidates':
        if f.get('profile_status'): where.append("c.profile_status = %s"); params.append(f['profile_status'])
        if f.get('calling_status'): where.append("c.calling_status = %s"); params.append(f['calling_status'])
        if f.get('added_by'):      where.append("c.added_by = %s"); params.append(f['added_by'])
        if f.get('client_name'):   where.append("r.client_name = %s"); params.append(f['client_name'])
    else:
        if f.get('status'):       where.append("r.status = %s"); params.append(f['status'])
        if f.get('assigned_to'):  where.append("r.assigned_to ILIKE %s"); params.append(f"%{f['assigned_to']}%")
        if f.get('client_name'):  where.append("r.client_name = %s"); params.append(f['client_name'])

    sql = f"""
        SELECT {gexpr} AS label, COUNT(*) AS cnt
        FROM {table}
        {('WHERE ' + ' AND '.join(where)) if where else ''}
        GROUP BY {gexpr}
        ORDER BY 2 DESC
        LIMIT 50
    """

    try:
        with get_db_cursor() as (conn, cur):
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
        return jsonify({'rows':[dict(r) for r in rows]})
    except Exception:
        logger.exception("dashboard_query failed")
        return jsonify({'error':'server error'}), 500


# --- Backward-compat alias ---
@dashboard_bp.route('/dashboard_data')
def dashboard_data_alias():
    return dashboard_data_plus()


# === Requirement Pipeline Grid (canonical statuses) ===




from flask import Blueprint, session, request   # make sure 'request' is imported
# ... your other imports ...

@dashboard_bp.route('/dashboard_requirement_pipeline_grid')
def dashboard_requirement_pipeline_grid():
    if 'user_id' not in session:
        return "<div class='p-3 text-danger'>Unauthenticated.</div>", 401
    try:
        me = (session.get('username') or '').strip()
        is_recruiter = (session.get('role') == 'recruiter')
        client_q = (request.args.get('client') or '').strip()

        # WHERE builder
        where = []
        params = []
        # if is_recruiter and me:
         #   where.append("r.assigned_to ILIKE %s")
          #  params.append(f"%{me}%")
        where.append("r.status = 'Active'")
        if client_q:
            for term in client_q.split():
                where.append("TRIM(COALESCE(r.client_name,'')) ILIKE %s")
                params.append(f"%{term}%")
        where_sql = " WHERE " + " AND ".join(where) if where else ""

        sql = f"""
            SELECT
                r.id AS req_id,
                r.requirement_name,
                r.client_name,
                COALESCE(NULLIF(TRIM(c.profile_status),''), 'Others') AS profile_status,
                COUNT(c.id) AS cnt
            FROM requirements r
            LEFT JOIN candidates c ON c.requirement_id = r.id
            {where_sql}
            GROUP BY r.id, r.requirement_name, r.client_name,
                     COALESCE(NULLIF(TRIM(c.profile_status),''), 'Others')
            ORDER BY r.requirement_name
        """
        with get_db_cursor() as (conn, cur):
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []

        # Canonical buckets + patterns
        CANON = [
            'Profile shared with client','Review Pending by client','R1 to be schedule','R1 scheduled',
            'R2 Pending','R2 scheduled','R2 FBP','R2 Select','R3 Pending','R3 scheduled','R3 FBP',
            'HR Round','Offered','R1 FBP','R1 Rejected','R2 Rejected','R3 Rejected','Others'
        ]
        import re as _re
        PATTERNS = {
            'Profile shared with client':[r'profile\s*shared',r'shared.*client'],
            'Review Pending by client':[r'review\s*pending.*client',r'pending.*client'],
            'R1 to be schedule':[r'\br?1\b.*to\s*be.*sched'],
            'R1 scheduled':[r'\br?1\b.*sched'],
            'R2 Pending':[r'\br?2\b.*pending'],
            'R2 scheduled':[r'\br?2\b.*sched'],
            'R2 FBP':[r'\br?2\b.*fbp'],
            'R2 Select':[r'\br?2\b.*select'],
            'R3 Pending':[r'\br?3\b.*pending'],
            'R3 scheduled':[r'\br?3\b.*sched'],
            'R3 FBP':[r'\br?3\b.*fbp'],
            'HR Round':[r'hr\s*round',r'\bhr\b'],
            'Offered':[r'offered?',r'\boffer\b'],
            'R1 FBP':[r'\br?1\b.*fbp'],
            'R1 Rejected':[r'\br?1\b.*reject'],
            'R2 Rejected':[r'\br?2\b.*reject'],
            'R3 Rejected':[r'\br?3\b.*reject'],
        }
        def canonize(raw):
            t = (raw or '').strip().lower()
            for key,pats in PATTERNS.items():
                for pat in pats:
                    if _re.search(pat,t):
                        return key
            return 'Others'

        buckets = CANON[:]
        by_req = {}
        for r in rows:
            rid = r['req_id']
            if rid not in by_req:
                by_req[rid] = {
                    'requirement_name': r['requirement_name'],
                    'client_name': r['client_name'],
                    'counts': {k:0 for k in buckets}
                }
            k = canonize(r['profile_status'])
            by_req[rid]['counts'][k] += int(r['cnt'])

        def esc(v):
            return ('' if v is None else str(v)).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

        def _h(v):
            s = '' if v is None else str(v)
            return (s.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;').replace('"','&quot;'))

        # Build client options server-side (also used as a fallback if JS doesn't run)
        clients = []
        try:
            with get_db_cursor() as (conn, cur):
                cur.execute("""
                    SELECT DISTINCT COALESCE(NULLIF(TRIM(client_name), ''), 'Unknown') AS client_name
                    FROM requirements
                    WHERE status = 'Active'
                    ORDER BY 1
                """)
                rows_clients = cur.fetchall() or []
                clients = [r['client_name'] for r in rows_clients]
        except Exception:
            clients = []

        client_options_html = ["<option value=''>All clients</option>"]
        for cn in clients:
            val = _h(cn)
            txt = _h(cn)
            sel_attr = " selected" if cn == client_q else ""
            client_options_html.append(f"<option value=\"{val}\"{sel_attr}>{txt or '—'}</option>")
        client_options_html = "".join(client_options_html)

        # --- HTML OUTPUT ---
        out = []
        
        # Wire the change handler (in case the page-level JS didn't run)
        out.append("""
<script>(function(){
  var sel = document.getElementById('rpClientSel');
  if(!sel || sel._wired) return;
  sel.addEventListener('change', async function(){
    var val = sel.value || '';
    var base = '/dashboard_requirement_pipeline_grid';
    var url = val ? (base + '?client=' + encodeURIComponent(val)) : base;
    var spin = document.getElementById('drillSpinner');
    var cont = document.getElementById('drillContent');
    if(spin) spin.classList.remove('d-none');
    if(cont) cont.classList.add('d-none');
    try{
      var res = await fetch(url, {headers:{'X-Requested-With':'XMLHttpRequest'}});
      var html = await res.text();
      if(spin) spin.classList.add('d-none');
      if(cont){ cont.classList.remove('d-none'); cont.innerHTML = html; }
    }catch(e){ if(spin) spin.classList.add('d-none'); }
  }, {passive:true});
  sel._wired = true;
})();</script>
""")

        out.append("""<div class='table-responsive'>""")
        out.append("""<table class='table table-sm table-bordered align-middle rp-table'>""")
        out.append("""<thead class='table-light'><tr>""")
        out.append("""<th>Requirement</th><th>Client</th>""")
        for k in buckets:
            out.append(f"<th class='text-center'>{esc(k)}</th>")
        out.append("""<th class='text-center'>Total</th></tr></thead><tbody>""")

        for rid,row in sorted(by_req.items(), key=lambda kv:(kv[1]['requirement_name'] or '')):
            total = 0
            out.append(f"<tr><td>{esc(row['requirement_name'])}</td><td>{esc(row['client_name'])}</td>")
            for k in buckets:
                v = int(row['counts'].get(k, 0))
                total += v
                if v > 0 and k != "Others":
                    out.append(f"<td class='text-center'><a href='#' class='rp-link' data-req='{rid}' data-status='{esc(k)}'>{v}</a></td>")
                else:
                    cls = "text-center" + (" text-muted" if v == 0 else "")
                    out.append(f"<td class='{cls}'>{v}</td>")
            out.append(f"<td class='text-center fw-semibold'>{total}</td></tr>")

        out.append("""</tbody></table></div>""")
        return "".join(out)

    except Exception:
        logger.exception("Error building requirement pipeline grid")
        return "<div class='p-3 text-danger'>Server error.</div>", 500
@dashboard_bp.route('/dashboard_requirement_pipeline_table')
def dashboard_requirement_pipeline_table():
    if 'user_id' not in session:
        return "<div class='p-3 text-danger'>Unauthenticated.</div>", 401
    try:
        req_id = (request.args.get('req_id') or '').strip()
        canon = (request.args.get('status') or '').strip()
        if not req_id:
            return "<div class='p-3'>Missing requirement id.</div>", 400

        # token map aligns with the grid
        TOKENS = {
            'Profile shared with client': ['profile','shared','client'],
            'Review Pending by client':  ['review','pending','client'],
            'R1 to be schedule':         ['r1','to','be','sched'],
            'R1 scheduled':              ['r1','sched'],
            'R2 Pending':                ['r2','pending'],
            'R2 scheduled':              ['r2','sched'],
            'R2 FBP':                    ['r2','fbp'],
            'R2 Select':                 ['r2','select'],
            'R3 Pending':                ['r3','pending'],
            'R3 scheduled':              ['r3','sched'],
            'R3 FBP':                    ['r3','fbp'],
            'HR Round':                  ['hr'],
            'Offered':                   ['offer'],
            'R1 FBP':                    ['r1','fbp'],
            'R1 Rejected':               ['r1','reject'],
            'R2 Rejected':               ['r2','reject'],
            'R3 Rejected':               ['r3','reject'],
            'Others':                    None,
        }

        me = (session.get('username') or '').strip()
        is_recruiter = (session.get('role') == 'recruiter')

        where = ["r.id = %s"]
        params = [req_id]
        if is_recruiter and me:
            where.append("r.assigned_to ILIKE %s")
            params.append(f"%{me}%")
        tokens = TOKENS.get(canon)
        if tokens:
            ors = []
            for t in tokens:
                ors.append("LOWER(c.profile_status) LIKE %s")
                params.append('%' + t + '%')
            where.append("(" + " AND ".join(ors) + ")")
        elif canon and canon != 'Others':
            where.append("LOWER(COALESCE(NULLIF(TRIM(c.profile_status),''),'others')) = %s")
            params.append(canon.lower())

        sql = f"""
            SELECT
                c.id,
                c.candidate_name,
                c.emails,
                c.phones,
                c.current_company,
                c.current_location,
                COALESCE(NULLIF(TRIM(c.notice_period),''), '-') AS notice_period,
                COALESCE(NULLIF(TRIM(c.profile_status),''), 'Others') AS profile_status,
                COALESCE(NULLIF(TRIM(c.calling_status),''), '-') AS calling_status,
                COALESCE(NULLIF(TRIM(c.added_by),''), '-') AS added_by,
                to_char(c.added_date, 'YYYY-MM-DD') AS added_date
            FROM candidates c
            JOIN requirements r ON r.id = c.requirement_id
            WHERE {' AND '.join(where)}
            ORDER BY c.added_date DESC NULLS LAST, c.candidate_name
        """
        with get_db_cursor() as (conn, cur):
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []

        def _normalize_list_field_for_display(value):
            import json as _json, re as _re
            if value is None:
                return []
            if isinstance(value, (list, tuple)):
                items = [str(x).strip() for x in value if str(x).strip()]
            else:
                s = str(value).strip()
                if s.startswith('[') and s.endswith(']'):
                    try:
                        arr = _json.loads(s)
                        items = [str(x).strip() for x in arr if str(x).strip()]
                    except Exception:
                        items = [s] if s else []
                else:
                    parts = _re.split(r'[;,|]+', s)
                    items = [p.strip() for p in parts if p.strip()]
            cleaned = []
            for it in items:
                it = str(it)
                if it.endswith('.0') and it[:-2].isdigit():
                    it = it[:-2]
                cleaned.append(it)
            return cleaned
    

        def esc(v):
            return ('' if v is None else str(v)).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

        out = []
        out.append("<div class='table-responsive'>")
        out.append("<table class='table table-sm table-striped align-middle'>")
        out.append("<thead class='table-light'><tr>")
        out.append("<th>Name</th><th>Email</th><th>Phone</th><th>Company</th><th>Location</th><th>Notice Period</th><th>Profile Status</th><th>Calling Status</th><th>Added By</th><th>Added Date</th>")
        out.append("</tr></thead><tbody>")
        for r in rows:
            out.append("<tr>"f"<td>{esc(r.get('candidate_name'))}</td>"f"<td>{esc(', '.join(_normalize_list_field_for_display(r.get('emails')) or ['-']))}</td>"f"<td>{esc(', '.join(_normalize_list_field_for_display(r.get('phones')) or ['-']))}</td>"f"<td>{esc(r.get('current_company'))}</td>"f"<td>{esc(r.get('current_location'))}</td>"f"<td>{esc(r.get('notice_period'))}</td>"f"<td>{esc(r.get('profile_status'))}</td>"f"<td>{esc(r.get('calling_status'))}</td>"f"<td>{esc(r.get('added_by'))}</td>"f"<td>{esc(r.get('added_date'))}</td></tr>")
        if not rows:
            out.append("<tr><td colspan='10' class='text-center text-muted py-4'>No candidates found.</td></tr>")
        out.append("</tbody></table></div>")
        return "".join(out)
    except Exception:
        logger.exception("Error building requirement pipeline table")
        return "<div class='p-3 text-danger'>Server error.</div>", 500


@dashboard_bp.route('/dashboard_clients')
def dashboard_clients():
    if 'user_id' not in session:
        return jsonify({'error': 'unauthenticated'}), 401
    try:
        me = (session.get('username') or '').strip()
        is_recruiter = (session.get('role') == 'recruiter')
        where = ["r.status = 'Active'"]
        params = []
        if is_recruiter and me:
            where.append("r.assigned_to ILIKE %s")
            params.append(f"%{me}%")
        sql = f"""
            SELECT DISTINCT COALESCE(NULLIF(TRIM(r.client_name), ''), 'Unknown') AS client_name
            FROM requirements r
            WHERE {' AND '.join(where)}
            ORDER BY 1
        """
        with get_db_cursor() as (conn, cur):
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
        return jsonify({'clients': [r['client_name'] for r in rows]})
    except Exception:
        logger.exception("Error loading client list")
        return jsonify({'error': 'server error'}), 500


# === Recruiter Pipeline (pivot by recruiter × candidate profile_status) ===
@dashboard_bp.route('/dashboard_recruiter_pipeline_grid')
def dashboard_recruiter_pipeline_grid():
    try:
        # Show ALL users' data to both admin and recruiter (no role scoping).
        with get_db_cursor() as (conn, cur):
            cur.execute("""
                SELECT
                    COALESCE(NULLIF(TRIM(c.added_by),''), 'Unknown') AS recruiter_name,
                    COALESCE(NULLIF(TRIM(c.profile_status),''), 'Others') AS profile_status,
                    COUNT(*)::int AS cnt
                FROM candidates c
                GROUP BY 1, 2
                ORDER BY 1
            """)
            rows = cur.fetchall() or []

        CANON = [
            'Profile shared with client','Review Pending by client','R1 to be schedule','R1 scheduled',
            'R2 Pending','R2 scheduled','R2 FBP','R2 Select','R3 Pending','R3 scheduled','R3 FBP',
            'HR Round','Offered','R1 FBP','R1 Rejected','R2 Rejected','R3 Rejected','Others'
        ]
        import re as _re
        PATTERNS = {
            'Profile shared with client':[r'profile\s*shared',r'shared.*client'],
            'Review Pending by client':[r'review\s*pending.*client',r'pending.*client'],
            'R1 to be schedule':[r'\br?1\b.*to\s*be.*sched'],
            'R1 scheduled':[r'\br?1\b.*sched'],
            'R2 Pending':[r'\br?2\b.*pending'],
            'R2 scheduled':[r'\br?2\b.*sched'],
            'R2 FBP':[r'\br?2\b.*fbp'],
            'R2 Select':[r'\br?2\b.*select'],
            'R3 Pending':[r'\br?3\b.*pending'],
            'R3 scheduled':[r'\br?3\b.*sched'],
            'R3 FBP':[r'\br?3\b.*fbp'],
            'HR Round':[r'hr\s*round',r'\bhr\b'],
            'Offered':[r'offered?',r'\boffer\b'],
            'R1 FBP':[r'\br?1\b.*fbp'],
            'R1 Rejected':[r'\br?1\b.*reject'],
            'R2 Rejected':[r'\br?2\b.*reject'],
            'R3 Rejected':[r'\br?3\b.*reject'],
        }
        def canonize(raw):
            t = (raw or '').strip().lower()
            for key,pats in PATTERNS.items():
                for pat in pats:
                    if _re.search(pat,t):
                        return key
            return 'Others'

        buckets = CANON[:]
        by_rec = {}
        for r in rows:
            rec = r['recruiter_name'] or 'Unknown'
            if rec not in by_rec:
                by_rec[rec] = { 'counts': {k:0 for k in buckets} }
            k = canonize(r['profile_status'])
            by_rec[rec]['counts'][k] += int(r['cnt'])

        def esc(v):
            return ('' if v is None else str(v)).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

        out = []
        out.append("""<div class='table-responsive'>""")
        out.append("""<table class='table table-sm table-bordered align-middle rp2-table'>""")
        out.append("""<thead class='table-light'><tr>""")
        out.append("<th>Recruiter</th>")
        for k in buckets:
            out.append(f"<th class='text-center'>{esc(k)}</th>")
        out.append("<th class='text-center'>Total</th></tr></thead><tbody>")

        for rec in sorted(by_rec.keys(), key=lambda x:(x or '').lower()):
            row = by_rec[rec]
            total = 0
            out.append(f"<tr><td>{esc(rec)}</td>")
            for k in buckets:
                v = int(row['counts'].get(k,0))
                total += v
                if v > 0 and k != "Others":
                    out.append(f"<td class='text-center'><a href='#' class='rp2-link' data-recruiter='{esc(rec)}' data-status='{esc(k)}'>{v}</a></td>")
                else:
                    cls = "text-center" + (" text-muted" if v == 0 else "")
                    out.append(f"<td class='{cls}'>{v}</td>")
            out.append(f"<td class='text-center fw-semibold'>{total}</td></tr>")

        out.append("</tbody></table></div>")
        return "".join(out)

    except Exception:
        logger.exception("Error building recruiter pipeline grid")
        return "<div class='p-3 text-danger'>Server error.</div>", 500


@dashboard_bp.route('/dashboard_recruiter_pipeline_table')
def dashboard_recruiter_pipeline_table():
    try:
        recruiter = (request.args.get('recruiter') or '').strip()
        canon = (request.args.get('status') or '').strip()
        if not recruiter or not canon:
            return "<div class='p-3'>Missing recruiter or status.</div>", 400

        TOKENS = {
            'Profile shared with client': ['profile','shared','client'],
            'Review Pending by client':  ['review','pending','client'],
            'R1 to be schedule':         ['r1','to','be','sched'],
            'R1 scheduled':              ['r1','sched'],
            'R2 Pending':                ['r2','pending'],
            'R2 scheduled':              ['r2','sched'],
            'R2 FBP':                    ['r2','fbp'],
            'R2 Select':                 ['r2','select'],
            'R3 Pending':                ['r3','pending'],
            'R3 scheduled':              ['r3','sched'],
            'R3 FBP':                    ['r3','fbp'],
            'HR Round':                  ['hr'],
            'Offered':                   ['offer'],
            'R1 FBP':                    ['r1','fbp'],
            'R1 Rejected':               ['r1','reject'],
            'R2 Rejected':               ['r2','reject'],
            'R3 Rejected':               ['r3','reject'],
            'Others':                    []
        }

        tokens = TOKENS.get(canon, [])
        where = ["COALESCE(NULLIF(TRIM(c.added_by),''), 'Unknown') = %s"]
        params = [recruiter]

        if tokens:
            ors = []
            for t in tokens:
                ors.append("LOWER(COALESCE(c.profile_status,'')) LIKE %s")
                params.append(f"%{t}%")
            where.append("(" + " OR ".join(ors) + ")")
        else:
            where.append(r"""NOT (
                LOWER(COALESCE(c.profile_status,'')) ~ '(profile\s*shared|shared.*client|review\s*pending.*client|pending.*client|\br?1\b.*sched|\br?1\b.*to\s*be.*sched|\br?2\b.*pending|\br?2\b.*sched|\br?2\b.*fbp|\br?2\b.*select|\br?3\b.*pending|\br?3\b.*sched|\br?3\b.*fbp|hr\s*round|\boffer\b|offered?|\br?1\b.*fbp|\br?1\b.*reject|\br?2\b.*reject|\br?3\b.*reject)'
            )""")

        sql = f"""
            SELECT
              c.id,
              c.candidate_name,
              c.job_title,
              c.current_company,
              c.current_location,
              c.profile_status,
              c.calling_status,
              c.interview_date,
              c.interview_time,
              c.added_by,
              c.added_date,
              r.requirement_name,
              r.client_name
            FROM candidates c
            LEFT JOIN requirements r ON r.id = c.requirement_id
            WHERE {" AND ".join(where)}
            ORDER BY COALESCE(c.added_date, NOW()) DESC
            LIMIT 1000
        """

        with get_db_cursor() as (conn, cur):
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []

        def esc(v):
            try:
                return '' if v is None else str(v)
            except Exception:
                return ''

        html = []
        html.append("""
        <div class="table-responsive">
          <table class="table table-bordered table-hover align-middle">
            <thead class="table-light">
              <tr>
                <th>ID</th>
                <th>Candidate</th>
                <th>Job Title</th>
                <th>Company</th>
                <th>Location</th>
                <th>Profile Status</th>
                <th>Calling Status</th>
                <th>Interview</th>
                <th>Requirement</th>
                <th>Client</th>
                <th>Added By</th>
                <th>Added On</th>
                <th>Open</th>
              </tr>
            </thead>
            <tbody>
        """)
        for r in rows:
            interview = ''
            if r.get('interview_date'):
                interview = esc(r.get('interview_date'))
                if r.get('interview_time'):
                    interview += f" {esc(r.get('interview_time'))}"
            try:
                url = url_for('view_candidate', cand_id=r['id'])
            except Exception:
                url = '#'
            html.append(f"""
              <tr>
                <td>{esc(r.get('id'))}</td>
                <td>{esc(r.get('candidate_name'))}</td>
                <td>{esc(r.get('job_title'))}</td>
                <td>{esc(r.get('current_company'))}</td>
                <td>{esc(r.get('current_location'))}</td>
                <td><span class="badge bg-primary">{esc(r.get('profile_status')) or '-'}</span></td>
                <td>{esc(r.get('calling_status')) or '-'}</td>
                <td>{interview or '-'}</td>
                <td>{esc(r.get('requirement_name'))}</td>
                <td>{esc(r.get('client_name'))}</td>
                <td>{esc(r.get('added_by')) or '-'}</td>
                <td>{esc(r.get('added_date')) or '-'}</td>
                <td><a class="btn btn-sm btn-outline-primary" href="{url}" target="_blank">View</a></td>
              </tr>
            """)
        html.append("""
            </tbody>
          </table>
        </div>
        """)
        return "".join(html)

    except Exception:
        logger.exception("Error building recruiter pipeline table")
        return "<div class='p-3 text-danger'>Server error while loading data.</div>", 500
