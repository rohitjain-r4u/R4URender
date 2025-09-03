
import os
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")


from flask import Blueprint, request, send_file, jsonify
import io
import pandas as pd
from datetime import datetime
import psycopg2
from contextlib import contextmanager

# --- Inline DB cursor (instead of importing from db.py) ---
@contextmanager
def get_db_cursor():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        cur = conn.cursor()
        yield conn, cur
        conn.commit()   # make sure changes persist
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

# --- Blueprint ---
export_bp = Blueprint("export_bp", __name__)

@export_bp.route("/export_candidates", methods=["POST"])
def export_candidates():
    data = request.get_json(silent=True) or {}
    ids = [int(i) for i in data.get("ids", []) if str(i).isdigit()]
    print("DEBUG ids:", ids, flush=True)

    if not ids:
        return jsonify({"error": "No candidate IDs provided"}), 400

    try:
        with get_db_cursor() as (conn, cur):
            q = "SELECT * FROM candidates WHERE id = ANY(%s)"
            cur.execute(q, (ids,))
            candidates = cur.fetchall()
            print("DEBUG row count:", len(candidates), flush=True)

        # Convert query results into dicts (psycopg2 returns tuples by default)
        colnames = [desc[0] for desc in cur.description]
        candidates = [dict(zip(colnames, row)) for row in candidates]

        # Normalize and build export rows
        rows = []
        for c in candidates:
            if isinstance(c.get("phones"), str):
                c["phones"] = c["phones"].strip("{}").split(",") if c["phones"] else []
            if isinstance(c.get("emails"), str):
                c["emails"] = c["emails"].strip("{}").split(",") if c["emails"] else []

            rows.append({
                "application_date": c.get("application_date", ""),
                "job_title": c.get("job_title", ""),
                "candidate_name": c.get("candidate_name", ""),
                "current_company": c.get("current_company", ""),
                "total_experience": c.get("total_experience", ""),
                "phones": ", ".join(c.get("phones") or []),
                "emails": ", ".join(c.get("emails") or []),
                "notice_period": c.get("notice_period", ""),
                "current_location": c.get("current_location", ""),
                "preferred_locations": c.get("preferred_locations", ""),
                "ctc_current": c.get("ctc_current", ""),
                "ectc": c.get("ectc", ""),
                "calling_status": c.get("calling_status", ""),
                "profile_status": c.get("profile_status", ""),
                "comments": c.get("comments", ""),
                "added_date": c.get("added_date", ""),
                "updated_date": c.get("updated_date", ""),
                "added_by": c.get("added_by", ""),
            })

        df = pd.DataFrame(rows, columns=[
            "application_date","job_title","candidate_name","current_company",
            "total_experience","phones","emails","notice_period","current_location",
            "preferred_locations","ctc_current","ectc","calling_status","profile_status",
            "comments","added_date","updated_date","added_by"
        ])

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Candidates")
        output.seek(0)

        filename = f"candidates_export_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        print("DEBUG error:", str(e), flush=True)
        return jsonify({"error": str(e)}), 500
