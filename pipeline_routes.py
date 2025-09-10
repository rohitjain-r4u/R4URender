"""
pipeline_routes.py
Blueprint for Pipeline Data page + API endpoints.
Requires: psycopg2-binary, Flask
"""

from contextlib import contextmanager
import psycopg2
import datetime
import decimal
import uuid


# --- automatic serialization helpers to convert DB types to JSON-safe primitives ---
def _serialize_value(v):
    if v is None:
        return None
    if isinstance(v, (datetime.datetime, datetime.date)):
        return v.isoformat()
    if isinstance(v, datetime.time):  # Added handling for time objects
        return v.isoformat()
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, uuid.UUID):
        return str(v)
    return v

def serialize_row(row):
    """Convert a DB row (mapping or sequence) to a dict with JSON-safe values."""
    try:
        d = dict(row)
    except Exception:
        return row
    return {k: _serialize_value(v) for k, v in d.items()}

import psycopg2.extras
from flask import Blueprint, render_template, request, jsonify, current_app
import os

bp = Blueprint('pipeline', __name__)

# DB config - env-driven (supports DATABASE_URL or individual DB_* vars)
DB_CONFIG = {}
DATABASE_URL = os.getenv('DATABASE_URL')
if DATABASE_URL:
    # Use psycopg2 connection string (DSN) when DATABASE_URL is provided
    DB_CONFIG['dsn'] = DATABASE_URL
else:
    # Fallback to individual environment variables (keeps local defaults)
    DB_CONFIG = {
        'dbname': os.getenv('DB_NAME', 'job_portal'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', ''),   # prefer setting this in env
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', '5432')),
    }

@contextmanager
def get_db_cursor(commit_on_exit=False):
    \"\"\"Yield (conn, cur). Uses DB_CONFIG; supports DB_CONFIG['dsn'] (DATABASE_URL).\"\"\"
    conn = None
    cur = None
    try:
        # If a DSN string is present, pass that to psycopg2.connect, otherwise expand kwargs.
        if 'dsn' in DB_CONFIG:
            conn = psycopg2.connect(DB_CONFIG['dsn'])
        else:
            conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        yield conn, cur
        if commit_on_exit:
            conn.commit()
    except Exception:
        # If a commit was intended but we failed, attempt rollback to avoid partial writes.
        if conn and commit_on_exit:
            try:
                conn.rollback()
            except Exception:
                pass
        # Re-raise so calling code handles/logs the exception as before
        raise
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass



# ---- Routes ----

@bp.route('/pipeline-data')
def pipeline_page():
    """
    Render the pipeline_data.html template.
    Make sure templates/pipeline_data.html exists.
    """
    # Defensive: if template missing, provide a helpful 500 page instead of dumping Python
    templates_dir = os.path.join(os.path.dirname(__file__), 'templates')
    template_path = os.path.join(templates_dir, 'pipeline_data.html')
    if not os.path.isfile(template_path):
        # Helpful error page so browser won't render Python source
        return (
            "<h2>Template missing</h2>"
            "<p>The template <code>templates/pipeline_data.html</code> was not found. "
            "Create the file in your project <strong>templates</strong> folder and restart the app.</p>"
        ), 500

    return render_template('pipeline_data.html')


@bp.route('/api/clients')
def api_clients():
    """
    Return JSON array of objects:
      [{ "client_name": "Client A", "active_requirements": 3 }, ...]
    Only clients with active requirements (status IS NULL or != 'closed') are returned.
    """
    try:
        with get_db_cursor() as (conn, cur):
            cur.execute("""
                SELECT client_name, COUNT(id) AS active_requirements
                FROM requirements
                WHERE (LOWER(TRIM(COALESCE(status, ''))) != 'closed')
                  AND client_name IS NOT NULL
                GROUP BY client_name
                ORDER BY client_name;
            """)
            rows = cur.fetchall()
    except Exception as e:
        try:
            current_app.logger.exception('api_clients query failed: %s', e)
        except Exception:
            pass
        return jsonify({'error': 'server_error'}), 500

    out = []
    for r in rows:
        name = r.get('client_name')
        count = int(r.get('active_requirements') or 0)
        out.append({'client_name': name, 'active_requirements': count})
    return jsonify(out)





@bp.route('/api/clients/all_summary')
def api_clients_all_summary():
    """Return total count of active requirements across all clients."""
    try:
        with get_db_cursor() as (conn, cur):
            cur.execute("""
                SELECT COUNT(id) AS total_active_requirements
                FROM requirements
                WHERE (LOWER(TRIM(COALESCE(status, ''))) != 'closed')
                  AND client_name IS NOT NULL
            """)
            row = cur.fetchone()
            total = int(row.get('total_active_requirements') or 0)
    except Exception as e:
        try:
            current_app.logger.exception('api_clients_all_summary failed: %s', e)
        except Exception:
            pass
        return jsonify({'error': 'server_error'}), 500
    return jsonify({'client_name': 'All Clients', 'active_requirements': total})


@bp.route('/api/client/requirements_details_all')
def api_requirements_details_all():
    """Return requirements details across all clients (active only)."""
    # Get pagination parameters
    try:
        page = max(1, int(request.args.get('page') or 1))
    except Exception:
        page = 1
    try:
        per_page = min(100, int(request.args.get('per_page') or 50))  # Reasonable default
    except Exception:
        per_page = 50
        
    offset = (page - 1) * per_page
    
    try:
        with get_db_cursor() as (conn, cur):
            # Get total count
            cur.execute("""
                SELECT COUNT(DISTINCT r.id) as total_count
                FROM requirements r
                WHERE (LOWER(TRIM(COALESCE(r.status, ''))) != 'closed')
            """)
            total_count = cur.fetchone().get('total_count', 0)
            
            # Get paginated data
            cur.execute(r"""
                SELECT
                  r.id AS requirement_id,
                  r.requirement_name,
                  COALESCE(r.client_name, '') AS client_name,
                  COALESCE(r.client_poc, '') AS client_poc,
                  COALESCE(r.assigned_to, '') AS assigned_to,
                  COUNT(c.id) AS total_candidates,
                  SUM(CASE WHEN c.added_date >= (NOW() - INTERVAL '7 days') THEN 1 ELSE 0 END) AS candidates_last_7_days,
                  SUM(CASE WHEN LOWER(TRIM(COALESCE(c.profile_status,''))) NOT LIKE '%%rejected%%' THEN 1 ELSE 0 END) AS candidates_not_rejected,
                  SUM(CASE WHEN LOWER(TRIM(COALESCE(c.profile_status,''))) IN ('r2 pending','r2 scheduled','r2 fbp') THEN 1 ELSE 0 END) AS r2_candidates,
                  SUM(CASE WHEN LOWER(TRIM(COALESCE(c.profile_status,''))) IN ('r2 select','r3 pending','r3 scheduled','r3 fbp') THEN 1 ELSE 0 END) AS r3_candidates,
                  SUM(CASE WHEN LOWER(TRIM(COALESCE(c.profile_status,''))) = 'hr round' THEN 1 ELSE 0 END) AS hr_rounds,
                  SUM(CASE WHEN LOWER(TRIM(COALESCE(c.profile_status,''))) = 'offered' THEN 1 ELSE 0 END) AS offered_count
                FROM requirements r
                LEFT JOIN candidates c ON c.requirement_id = r.id
                WHERE (LOWER(TRIM(COALESCE(r.status, ''))) != 'closed')
                GROUP BY r.id, r.requirement_name, r.client_name, r.client_poc, r.assigned_to
                ORDER BY r.added_date DESC
                LIMIT %s OFFSET %s
            """, (per_page, offset))
            rows = cur.fetchall()
    except Exception as e:
        try:
            current_app.logger.exception('api_requirements_details_all failed: %s', e)
        except Exception:
            pass
        return jsonify({'error': 'server_error'}), 500

    out = []
    for r in rows:
        out.append({
            'requirement_id': r.get('requirement_id'),
            'requirement_name': r.get('requirement_name'),
            'client_name': r.get('client_name'),
            'client_poc': r.get('client_poc'),
            'assigned_to': r.get('assigned_to'),
            'total_candidates': int(r.get('total_candidates') or 0),
            'candidates_last_7_days': int(r.get('candidates_last_7_days') or 0),
            'candidates_not_rejected': int(r.get('candidates_not_rejected') or 0),
            'r2_candidates': int(r.get('r2_candidates') or 0),
            'r3_candidates': int(r.get('r3_candidates') or 0),
            'hr_rounds': int(r.get('hr_rounds') or 0),
            'offered_count': int(r.get('offered_count') or 0),
        })
    
    return jsonify({
        'requirements': out,
        'pagination': {
            'page': page,
            'per_page': per_page,
            'total': total_count,
            'pages': (total_count + per_page - 1) // per_page
        }
    })


@bp.route('/api/client/requirements')
def api_client_requirements():
    """
    Return requirements for a given client.
    Basic fields: id, requirement_name, assigned_to, added_date, status
    """
    client_name = request.args.get('client_name')
    if not client_name:
        return jsonify({'error': 'client_name query parameter required'}), 400

    # Get pagination parameters
    try:
        page = max(1, int(request.args.get('page') or 1))
    except Exception:
        page = 1
    try:
        per_page = min(100, int(request.args.get('per_page') or 50))  # Reasonable default
    except Exception:
        per_page = 50
        
    offset = (page - 1) * per_page

    try:
        with get_db_cursor() as (conn, cur):
            # Get total count
            cur.execute("""
                SELECT COUNT(id) as total_count
                FROM requirements
                WHERE client_name = %s
                  AND (LOWER(TRIM(COALESCE(status, ''))) != 'closed')
            """, (client_name,))
            total_count = cur.fetchone().get('total_count', 0)
            
            # Get paginated data
            cur.execute("""
                SELECT id, requirement_name, assigned_to, added_date, COALESCE(status, '') AS status
                FROM requirements
                WHERE client_name = %s
                  AND (LOWER(TRim(COALESCE(status, ''))) != 'closed')
                ORDER BY added_date DESC
                LIMIT %s OFFSET %s
            """, (client_name, per_page, offset))
            reqs = cur.fetchall()
    except Exception as e:
        try:
            current_app.logger.exception('api_client_requirements failed: %s', e)
        except Exception:
            pass
        return jsonify({'error': 'server_error'}), 500

    out = []
    for r in reqs:
        out.append({
            'id': r.get('id'),
            'requirement_name': r.get('requirement_name'),
            'assigned_recruiter': r.get('assigned_to'),
            'added_date': r.get('added_date'),
            'status': r.get('status')
        })
    
    return jsonify({
        'client_name': client_name, 
        'requirements': out,
        'pagination': {
            'page': page,
            'per_page': per_page,
            'total': total_count,
            'pages': (total_count + per_page - 1) // per_page
        }
    })


@bp.route('/api/candidates')
def api_candidates():
    """
    Return candidates filtered by client_name OR requirement_id and optional count_type.
    Uses SQL conditions that match the aggregated counts. Supports pagination (limit, offset)
    and debug=1 for diagnostics.
    """
    client_name = request.args.get('client_name')
    requirement_id = request.args.get('requirement_id')
    count_type = (request.args.get('count_type') or '').strip().lower()
    debug_mode = request.args.get('debug') in ('1', 'true', 'yes', 'on')
    
    # Get pagination parameters
    try:
        page = max(1, int(request.args.get('page') or 1))
    except Exception:
        page = 1
    try:
        per_page = min(200, int(request.args.get('per_page') or 50))  # Reasonable default
    except Exception:
        per_page = 50
        
    offset = (page - 1) * per_page

    params = []
    where = []

    if requirement_id:
        where.append('c.requirement_id::text = %s')
        params.append(str(requirement_id))
    elif client_name:
        where.append("(r.client_name = %s AND (LOWER(TRIM(COALESCE(r.status, ''))) != 'closed'))")
        params.append(client_name)

    # Map count_type to SQL condition (must match requirements_details logic)
    if count_type == 'not_rejected':
        where.append("LOWER(TRIM(COALESCE(c.profile_status,''))) NOT LIKE %s")
        params.append('%rejected%')
    elif count_type == 'r2':
        where.append("LOWER(TRIM(COALESCE(c.profile_status,''))) IN ('r2 pending','r2 scheduled','r2 fbp')")
    elif count_type == 'r3':
        where.append("LOWER(TRIM(COALESCE(c.profile_status,''))) IN ('r2 select','r3 pending','r3 scheduled','r3 fbp')")
    elif count_type == 'hr':
        where.append("LOWER(TRIM(COALESCE(c.profile_status,''))) = 'hr round'")
    elif count_type == 'offered':
        where.append("LOWER(TRIM(COALESCE(c.profile_status,''))) = 'offered'")
    elif count_type == 'added':
        where.append("c.added_date >= (NOW() - INTERVAL '7 days')")

    where_sql = ''
    if where:
        where_sql = 'WHERE ' + ' AND '.join(where)

    join_clause = 'LEFT JOIN requirements r ON r.id = c.requirement_id' if client_name else ''

    # Count query
    count_q = f"""
        SELECT COUNT(*) as total_count
        FROM candidates c
        {join_clause}
        {where_sql}
    """

    # Data query
    data_q = f"""
        SELECT c.*
        FROM candidates c
        {join_clause}
        {where_sql}
        ORDER BY c.id DESC
        LIMIT %s OFFSET %s
    """

    params_for_count = tuple(params)
    params_for_data = tuple(params) + (per_page, offset)
    
    try:
        with get_db_cursor() as (conn, cur):
            # Get total count
            cur.execute(count_q, params_for_count)
            total_count = cur.fetchone().get('total_count', 0)
            
            # Get paginated data
            cur.execute(data_q, params_for_data)
            rows = cur.fetchall()
    except Exception as e:
        try:
            current_app.logger.exception('api_candidates sql failed: %s', e)
        except Exception:
            pass
        return jsonify({'error': 'server_error'}), 500

    # Serialize rows to handle non-JSON-serializable types like time
    serialized_rows = [serialize_row(row) for row in rows]

    if debug_mode:
        return jsonify({
            'debug': {
                'where_sql': where_sql, 
                'params': params, 
                'returned': len(serialized_rows)
            }, 
            'data': serialized_rows,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': total_count,
                'pages': (total_count + per_page - 1) // per_page
            }
        })

    return jsonify({
        'data': serialized_rows,
        'pagination': {
            'page': page,
            'per_page': per_page,
            'total': total_count,
            'pages': (total_count + per_page - 1) // per_page
        }
    })


@bp.route('/api/client/requirements_details')
def api_client_requirements_details():
    """
    Returns details per active requirement for a client with aggregated candidate counts.

    Response:
    {
      "client_name": "Acme",
      "requirements": [
         {
           "requirement_id": 123,
           "requirement_name": "Senior Dev",
           "client_poc": "PoC Name",
           "assigned_to": "Recruiter Name",
           "total_candidates": 10,
           "candidates_last_7_days": 2,
           "candidates_not_rejected": 8,
           "r2_candidates": 3,
           "r3_candidates": 1,
           "hr_rounds": 0,
           "offered_count": 1
         }, ...]
    }
    """
    client_name = request.args.get('client_name')
    if not client_name:
        return jsonify({'error': 'client_name query parameter required'}), 400

    # Get pagination parameters
    try:
        page = max(1, int(request.args.get('page') or 1))
    except Exception:
        page = 1
    try:
        per_page = min(100, int(request.args.get('per_page') or 50))  # Reasonable default
    except Exception:
        per_page = 50
        
    offset = (page - 1) * per_page

    try:
        with get_db_cursor() as (conn, cur):
            # Get total count
            cur.execute("""
                SELECT COUNT(DISTINCT r.id) as total_count
                FROM requirements r
                WHERE r.client_name = %s
                  AND (LOWER(TRIM(COALESCE(r.status, ''))) != 'closed')
            """, (client_name,))
            total_count = cur.fetchone().get('total_count', 0)
            
            # Get paginated data
            cur.execute(r"""
                SELECT
                  r.id AS requirement_id,
                  r.requirement_name,
                  COALESCE(r.client_poc, '') AS client_poc,
                  COALESCE(r.assigned_to, '') AS assigned_to,

                  COUNT(c.id) AS total_candidates,

                  SUM(CASE WHEN c.added_date >= (NOW() - INTERVAL '7 days') THEN 1 ELSE 0 END) AS candidates_last_7_days,

                  -- Updated logic: exclude any profile_status that *contains' 'rejected' (case-insensitive)
                  SUM(CASE WHEN LOWER(TRIM(COALESCE(c.profile_status, ''))) NOT LIKE '%%rejected%%' THEN 1 ELSE 0 END) AS candidates_not_rejected,

                  -- R2 candidates: r2 pending, r2 scheduled, r2 fbp
                  SUM(
                    CASE WHEN LOWER(TRIM(COALESCE(c.profile_status, ''))) IN ('r2 pending','r2 scheduled','r2 fbp')
                         THEN 1 ELSE 0 END
                  ) AS r2_candidates,

                  -- R3 candidates: r2 select, r3 pending, r3 scheduled, r3 fbp
                  SUM(
                    CASE WHEN LOWER(TRIM(COALESCE(c.profile_status, ''))) IN ('r2 select','r3 pending','r3 scheduled','r3 fbp')
                         THEN 1 ELSE 0 END
                  ) AS r3_candidates,

                  -- HR rounds
                  SUM(
                    CASE WHEN LOWER(TRIM(COALESCE(c.profile_status, ''))) = 'hr round' THEN 1 ELSE 0 END
                  ) AS hr_rounds,

                  -- Offered
                  SUM(
                    CASE WHEN LOWER(TRIM(COALESCE(c.profile_status, ''))) = 'offered' THEN 1 ELSE 0 END
                  ) AS offered_count

                FROM requirements r
                LEFT JOIN candidates c ON c.requirement_id = r.id
                WHERE r.client_name = %s
                  AND (LOWER(TRIM(COALESCE(r.status, ''))) != 'closed')
                GROUP BY r.id, r.requirement_name, r.client_poc, r.assigned_to
                ORDER BY r.added_date DESC
                LIMIT %s OFFSET %s
            """, (client_name, per_page, offset))
            rows = cur.fetchall()
    except Exception as e:
        try:
            current_app.logger.exception('api_client_requirements_details failed: %s', e)
        except Exception:
            pass
        return jsonify({'error': 'server_error'}), 500

    out = []
    for r in rows:
        out.append({
            'requirement_id': r.get('requirement_id'),
            'requirement_name': r.get('requirement_name'),
            'client_poc': r.get('client_poc'),
            'assigned_to': r.get('assigned_to'),
            'total_candidates': int(r.get('total_candidates') or 0),
            'candidates_last_7_days': int(r.get('candidates_last_7_days') or 0),
            'candidates_not_rejected': int(r.get('candidates_not_rejected') or 0),

            # new fields
            'r2_candidates': int(r.get('r2_candidates') or 0),
            'r3_candidates': int(r.get('r3_candidates') or 0),
            'hr_rounds': int(r.get('hr_rounds') or 0),
            'offered_count': int(r.get('offered_count') or 0),
        })
    
    return jsonify({
        'client_name': client_name, 
        'requirements': out,
        'pagination': {
            'page': page,
            'per_page': per_page,
            'total': total_count,
            'pages': (total_count + per_page - 1) // per_page
        }
    })




@bp.route('/api/candidate/<int:candidate_id>')
def api_candidate(candidate_id):
    """Return details for a single candidate by id."""
    try:
        with get_db_cursor() as (conn, cur):
            cur.execute('SELECT * FROM candidates WHERE id = %s LIMIT 1', (candidate_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({'error': 'not_found'}), 404
    except Exception as e:
        try:
            current_app.logger.exception('api_candidate failed: %s', e)
        except Exception:
            pass
        return jsonify({'error': 'server_error'}), 500
    return jsonify(serialize_row(row))

# New endpoint: aggregated client health summary for widgets
@bp.route('/api/client/health_summary')
def api_client_health_summary():
    """Return per-client health summary useful for UI widgets.
    Optional query param: client_name
    Response:
    [
      {
        'client_name': 'Acme',
        'active_requirements': 3,
        'total_candidates': 12,
        'rejected_count': 9,
        'last_status_change': '2025-09-05T14:32:10Z',
        'delayed_progress': true,
        'no_submissions': false,
        'less_submissions': false,
        'at_risk': true
      }, ...
    ]
    """
    try:
        client_name = None
        if request.args.get('client_name'):
            client_name = request.args.get('client_name')

        with get_db_cursor() as (conn, cur):
            sql = r"""
                SELECT
                  COALESCE(r.client_name, '') AS client_name,
                  COUNT(DISTINCT r.id) FILTER (WHERE LOWER(TRIM(COALESCE(r.status,''))) != 'closed') AS active_requirements,
                  COUNT(c.id) FILTER (WHERE LOWER(TRIM(COALESCE(r.status,''))) != 'closed') AS total_candidates,
                  SUM(CASE WHEN LOWER(TRIM(COALESCE(c.profile_status,''))) LIKE '%%rejected%%' THEN 1 ELSE 0 END) FILTER (WHERE LOWER(TRIM(COALESCE(r.status,''))) != 'closed') AS rejected_count,
                  MAX(c.updated_date) FILTER (WHERE LOWER(TRIM(COALESCE(r.status,''))) != 'closed') AS last_status_change
                FROM requirements r
                LEFT JOIN candidates c ON c.requirement_id = r.id
                {where_clause}
                GROUP BY COALESCE(r.client_name, '')
                ORDER BY COALESCE(r.client_name, '')
                LIMIT 1000
            """
            where_clause = ''
            params = []
            if client_name:
                where_clause = "WHERE r.client_name = %s"
                params = [client_name]
            sql = sql.format(where_clause=where_clause)
            cur.execute(sql, params)
            rows = [dict((k[0], _serialize_value(v)) for k,v in zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

        out = []
        now = datetime.datetime.utcnow()
        for r in rows:
            total = int(r.get('total_candidates') or 0)
            rejected = int(r.get('rejected_count') or 0)
            last_ts = r.get('last_status_change')
            # compute delayed_progress: no status change in last 3 working days (ignore Sat/Sun)
            delayed = False
            if last_ts:
                # parse timestamp
                if isinstance(last_ts, str):
                    try:
                        last_dt = datetime.datetime.fromisoformat(last_ts)
                    except Exception:
                        last_dt = None
                else:
                    last_dt = last_ts
                if last_dt:
                    # count working days between last_dt.date() and now.date()
                    dstart = last_dt.date()
                    dend = now.date()
                    workdays = 0
                    curday = dstart
                    while curday <= dend:
                        if curday.weekday() < 5:  # Mon-Fri
                            workdays += 1
                        curday = curday + datetime.timedelta(days=1)
                    # if more than 3 working days have passed (excluding current day), mark delayed
                    if workdays - 1 >= 3:
                        delayed = True
            # flags
            no_sub = (total == 0)
            less_sub = (total > 0 and total < 10)
            at_risk = (total > 0 and (rejected / float(total)) > 0.7)
            out.append({
                'client_name': r.get('client_name'),
                'active_requirements': int(r.get('active_requirements') or 0),
                'total_candidates': total,
                'rejected_count': rejected,
                'last_status_change': r.get('last_status_change'),
                'delayed_progress': delayed,
                'no_submissions': no_sub,
                'less_submissions': less_sub,
                'at_risk': at_risk
            })
    except Exception as e:
        try:
            current_app.logger.exception('api_client_health_summary failed: %s', e)
        except Exception:
            pass
        return jsonify({'error': 'server_error'}), 500
    return jsonify(out)