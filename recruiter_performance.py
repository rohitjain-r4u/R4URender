
from flask import Blueprint, render_template, jsonify, session
from contextlib import contextmanager
import psycopg2
import psycopg2.extras

# Blueprint name must match url_for usage in template
recruiter_perf_bp = Blueprint('recruiter_perf', __name__)

# Adjust if needed for your environment
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
    finally:
        if cur:
            try: cur.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass

# Query helpers
DATE_COL = "c.added_date"  # confirmed column name
JOIN_CLAUSE = """
FROM users u
LEFT JOIN candidates c
  ON LOWER(TRIM(c.added_by)) = LOWER(TRIM(u.username))
"""

@recruiter_perf_bp.route('/performance')
def recruiter_performance_page():
    """Renders the page. The template uses url_for() for API path, so no hardcoded URLs."""
    return render_template('recruiter_performance.html')

@recruiter_perf_bp.route('/api/performance')
def recruiter_performance_data():
    """Returns JSON stats for all recruiters/admins.
    Output shape:
      { "recruiters": [ { "username": str, "avatar": str|None,
                          "total_all_time": int, "today": int, "yesterday": int,
                          "avg_last_7": float, "avg_last_30": float } ] }
    """
    # Build SQL once; any DB errors are caught and returned as safe JSON
    sql_all = f"""
      SELECT u.username, u.avatar, COUNT(c.*)::int AS total_all_time
      {JOIN_CLAUSE}
      GROUP BY u.username, u.avatar
    """

    sql_today = f"""
      SELECT u.username, COUNT(c.*)::int AS cnt
      {JOIN_CLAUSE}
      WHERE ({DATE_COL})::date = CURRENT_DATE
      GROUP BY u.username
    """

    sql_yesterday = f"""
      SELECT u.username, COUNT(c.*)::int AS cnt
      {JOIN_CLAUSE}
      WHERE ({DATE_COL})::date = CURRENT_DATE - INTERVAL '1 day'
      GROUP BY u.username
    """

    sql_last7 = f"""
      SELECT u.username, COUNT(c.*)::int AS cnt
      {JOIN_CLAUSE}
      WHERE ({DATE_COL})::date >= CURRENT_DATE - INTERVAL '6 days'
      GROUP BY u.username
    """

    sql_last30 = f"""
      SELECT u.username, COUNT(c.*)::int AS cnt
      {JOIN_CLAUSE}
      WHERE ({DATE_COL})::date >= CURRENT_DATE - INTERVAL '29 days'
      GROUP BY u.username
    """

    data = {}
    try:
        with get_db_cursor() as (conn, cur):
            # All-time
            cur.execute(sql_all, ()); rows = cur.fetchall() or []
            for r in rows:
                u = r['username']
                data[u] = {
                    'username': u,
                    'avatar': r.get('avatar'),
                    'total_all_time': r['total_all_time']
                }
            # Helper to merge
            def merge(key, rows):
                for r in rows:
                    u = r['username']
                    data.setdefault(u, {'username': u, 'avatar': None, 'total_all_time': 0})
                    data[u][key] = r['cnt']
            # Day buckets
            cur.execute(sql_today, ()); merge('today', cur.fetchall() or [])
            cur.execute(sql_yesterday, ()); merge('yesterday', cur.fetchall() or [])
            cur.execute(sql_last7, ()); merge('last7_total', cur.fetchall() or [])
            cur.execute(sql_last30, ()); merge('last30_total', cur.fetchall() or [])
        # Derived avgs
        for u, rec in list(data.items()):
            rec['today'] = rec.get('today', 0)
            rec['yesterday'] = rec.get('yesterday', 0)
            rec['avg_last_7'] = round((rec.get('last7_total', 0) or 0) / 5.0, 1)
            rec['avg_last_30'] = round((rec.get('last30_total', 0) or 0) / 21.0, 1)
            rec.pop('last7_total', None); rec.pop('last30_total', None)
    except Exception as e:
        # If something failed (e.g., wrong column), include message and continue with zeros
        err = f"{type(e).__name__}: {e}"
        # Try to list recruiters anyway so UI has entries
        try:
            with get_db_cursor() as (conn, cur):
                cur.execute("SELECT username, avatar FROM users WHERE role IN ('recruiter','admin') ORDER BY username ASC")
                for r in (cur.fetchall() or []):
                    u = r['username']
                    data.setdefault(u, {
                        'username': u,
                        'avatar': r.get('avatar'),
                        'total_all_time': 0,
                        'today': 0, 'yesterday': 0,
                        'avg_last_7': 0.0, 'avg_last_30': 0.0
                    })
        except Exception:
            pass
        # return with error info to help troubleshooting on the client if needed
        return jsonify({'recruiters': sorted(data.values(), key=lambda x: x['username']),
                        'error': err}), 200

    # Ensure zero entries exist for recruiters with no candidates
    try:
        with get_db_cursor() as (conn, cur):
            cur.execute("SELECT username, avatar FROM users WHERE role IN ('recruiter','admin') ORDER BY username ASC")
            for r in (cur.fetchall() or []):
                u = r['username']
                data.setdefault(u, {
                    'username': u,
                    'avatar': r.get('avatar'),
                    'total_all_time': 0,
                    'today': 0, 'yesterday': 0,
                    'avg_last_7': 0.0, 'avg_last_30': 0.0
                })
    except Exception:
        pass

    resp = sorted(data.values(), key=lambda x: (-x.get('total_all_time', 0), x['username']))
    return jsonify({'recruiters': resp}), 200
