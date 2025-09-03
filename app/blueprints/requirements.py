"""
Domain blueprint: requirements
Extracted from main blueprint without changing logic.
"""
from flask import Blueprint
from ..core import *  # import original helpers and app context
bp = Blueprint("requirements", __name__)

@bp.route("/requirement/<int:req_id>/candidates/import", methods=["GET"])
def candidates_import(req_id):

    # Renders a dedicated page instead of a modal
    return render_template("import_wizard.html", req_id=req_id)

# -------------------------------------------------------------------
# (2,3,4,5,12) IMPORT PARSE (mapping), PREVIEW & COMMIT
# Column mapping UI before validation; preserve sheet order; browser
# gets sample values; server validates on commit; 'draft' option saves
# only valid rows and returns invalid back to client.
# -------------------------------------------------------------------
COLUMN_ALIASES = {
    "application_date": {"application date", "date of application", "applied on"},
    "candidate_name": {"name", "candidate", "full name"},
    "mobile": {"phone", "mobile number"},
    "email": {"mail", "email id"},
    "current_company": {"curr. company name", "current company", "company"},
    "education": {"under graduation degree", "degree", "qualification"},
    "pf_docs_confirm": {"ans(do you have all pf and other documents ...)", "pf docs confirm"},
    "notice_period_details": {"ans(what is your notice period? ...)", "notice period"},
    "ctc_current": {"ns(what is your current ctc in lakhs per annum?)", "current ctc"},
    "expected_ctc_lpa": {"ans(what is your expected ctc in lakhs per annum?)", "expected ctc"},
    "employee_size": {"ans(what is the employee size ...)", "employee size"},
    "companies_worked": {"ans(how many companies ...)", "companies worked"},
}

SYSTEM_FIELDS = [
    "application_date","candidate_name","mobile","email","current_company",
    "education","pf_docs_confirm","notice_period_details","ctc_current",
    "expected_ctc_lpa","employee_size","companies_worked","calling_status","profile_status",
]

def _suggest_map(header: str) -> str | None:
    h = (header or "").strip().lower()
    for sys, aliases in COLUMN_ALIASES.items():
        if h == sys or h in aliases:
            return sys
    return None

def _clean_bool_like(v):
    if v is None: return None
    s = str(v).strip().lower()
    if s in ("yes","y","true","1"): return "Yes"
    if s in ("no","n","false","0"): return "No"
    return v

def _normalize_row_smart(mapped_row: dict) -> dict:
    # Default application_date (6)
    if not mapped_row.get("application_date"):
        mapped_row["application_date"] = date.today().isoformat()
    # Normalize pf_docs_confirm
    if "pf_docs_confirm" in mapped_row:
        mapped_row["pf_docs_confirm"] = _clean_bool_like(mapped_row.get("pf_docs_confirm"))
    return mapped_row

def _server_side_row_validate(mapped_row: dict) -> list[str]:
    errors = []
    # Keep validations functional/UX only (no phone/email/ctc strictness per request)
    if not mapped_row.get("candidate_name"):
        errors.append("Candidate name is required.")
    # application_date basic format
    ad = mapped_row.get("application_date")
    try:
        if ad: datetime.fromisoformat(str(ad))
    except Exception:
        errors.append("Invalid application_date (use YYYY-MM-DD).")
    # controlled vocab (10)
    CALLING_ALLOWED = {"Not answering","Not reachable","Disconnected","Screen select"}
    PROFILE_ALLOWED = {"R2 Pending","R3 Pending","R1 to be schedule","R2 scheduled","R3 scheduled",
                       "R1 scheduled","R1 FBP","R2 FBP","R3 FBP","HR Round Pending","HR round done",
                       "Offer letter Pending","Offer letter released","Draft offer released","Drop"}
    if mapped_row.get("calling_status") and mapped_row["calling_status"] not in CALLING_ALLOWED:
        errors.append("calling_status must use predefined options.")
    if mapped_row.get("profile_status") and mapped_row["profile_status"] not in PROFILE_ALLOWED:
        errors.append("profile_status must use predefined options.")
    return errors

@app.post("/api/import/parse")
def api_import_parse():
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "No file"}), 400
    f = request.files["file"]
    content = f.read()
    df = pd.read_excel(io.BytesIO(content))
    headers = list(df.columns)
    suggestions = []
    for col in headers:
        suggestions.append({
            "sheet_column": col,
            "suggested_system_field": _suggest_map(col),
            "samples": [str(x) for x in (df[col].head(3).tolist())]
        })
    # DO NOT drop unmapped columns silently (2)
    resp = {
        "ok": True,
        "columns": suggestions,      # preserve order (3)
        "system_fields": SYSTEM_FIELDS,
        "message": "Review and confirm column mapping. Unmapped columns are NOT dropped until you choose 'No need to add'."
    }
    return jsonify(resp)

@app.post("/api/import/commit")
@csrf_protect
def api_import_commit():
    data = request.json or {}
    req_id = int(data.get("req_id", 0))
    mappings = data.get("mappings", [])  # list of {sheet_column, system_field or null/'ignore'}
    rows = data.get("rows", [])          # [{sheet_col:value,...}, ...] from client preview
    save_mode = data.get("mode", "all")  # "all" or "draft" (5)

    # Build a map
    colmap = {}
    used = set()
    for m in mappings:
        sysf = m.get("system_field")
        if not sysf or sysf in ("ignore", "No need to add"):
            continue
        if sysf in used:
            return jsonify({"ok": False, "error": f"Duplicate mapping to {sysf}"}), 400
        colmap[m["sheet_column"]] = sysf
        used.add(sysf)

    inserted, invalid = 0, []
    # DB cursor assumed
    cur = get_db().cursor()  # replace with your own helper
    for idx, r in enumerate(rows):
        mapped = { colmap[k]: v for k, v in r.items() if k in colmap }
        mapped = _normalize_row_smart(mapped)
        errs = _server_side_row_validate(mapped)
        if errs:
            invalid.append({"row_index": idx, "errors": errs})
            continue
        try:
            cur.execute("""
                INSERT INTO candidates (requirement_id, application_date, candidate_name, mobile, email,
                    current_company, education, pf_docs_confirm, notice_period_details, ctc_current,
                    expected_ctc_lpa, employee_size, companies_worked, calling_status, profile_status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                req_id,
                mapped.get("application_date"),
                mapped.get("candidate_name"),
                mapped.get("mobile"),
                mapped.get("email"),
                mapped.get("current_company"),
                mapped.get("education"),
                mapped.get("pf_docs_confirm"),
                mapped.get("notice_period_details"),
                mapped.get("ctc_current"),
                mapped.get("expected_ctc_lpa"),
                mapped.get("employee_size"),
                mapped.get("companies_worked"),
                mapped.get("calling_status"),
                mapped.get("profile_status"),
            ))
            inserted += 1
        except Exception as e:
            invalid.append({"row_index": idx, "errors": [f"DB error: {e}"]})
    if save_mode == "draft" and invalid:
        get_db().commit()
        return jsonify({"ok": True, "inserted": inserted, "invalid": invalid, "message":"Draft saved: valid rows inserted; fix remaining and re-save."})
    get_db().commit()
    return jsonify({"ok": True, "inserted": inserted, "invalid": invalid})

# -------------------------------------------------------------------
# (9) QUICK VIEW PARTIAL FOR SIDE PANEL
# -------------------------------------------------------------------
@app.get("/candidate/<int:candidate_id>/partial")
def candidate_partial(candidate_id):
    cur = get_db().cursor()
    cur.execute("SELECT * FROM candidates WHERE id=%s", (candidate_id,))
    row = cur.fetchone()
    if not row: abort(404)
    return render_template("candidate_detail_partial.html", candidate=row)

# -------------------------------------------------------------------
# (10) PREDEFINED DROPDOWNS / STATUS UPDATE API
# -------------------------------------------------------------------
CALLING_STATUS_CHOICES = ["Not answering","Not reachable","Disconnected","Screen select"]
PROFILE_STATUS_CHOICES = ["R2 Pending","R3 Pending","R1 to be schedule","R2 scheduled","R3 scheduled",
                          "R1 scheduled","R1 FBP","R2 FBP","R3 FBP","HR Round Pending","HR round done",
                          "Offer letter Pending","Offer letter released","Draft offer released","Drop"]

@app.post("/api/candidates/<int:candidate_id>/status")
def api_update_status(candidate_id):
    payload = request.json or {}
    calling = payload.get("calling_status")
    profile = payload.get("profile_status")
    if calling and calling not in CALLING_STATUS_CHOICES:
        return jsonify({"ok": False, "error":"Invalid calling_status"}), 400
    if profile and profile not in PROFILE_STATUS_CHOICES:
        return jsonify({"ok": False, "error":"Invalid profile_status"}), 400
    cur = get_db().cursor()
    if calling and profile:
        cur.execute("UPDATE candidates SET calling_status=%s, profile_status=%s WHERE id=%s",
                    (calling, profile, candidate_id))
    elif calling:
        cur.execute("UPDATE candidates SET calling_status=%s WHERE id=%s", (calling, candidate_id))
    elif profile:
        cur.execute("UPDATE candidates SET profile_status=%s WHERE id=%s", (profile, candidate_id))
    else:
        return jsonify({"ok": False, "error":"Nothing to update"}), 400
    get_db().commit()
    return jsonify({"ok": True})

# -------------------------------------------------------------------
# (6) MANUAL ADD/EDIT: DEFAULT application_date IF BLANK
# (Find your add/edit handlers and insert this normalization)
# -------------------------------------------------------------------
def _ensure_application_date(payload: dict):
    if not payload.get("application_date"):
        payload["application_date"] = date.today().isoformat()
    return payload

# Example hook inside your add-candidate POST:
# form = request.form.to_dict()
# form = _ensure_application_date(form)
# ... proceed to insert using form["application_date"]

# -------------------------------------------------------------------
# (7,8) USER PREFERENCES for Saved Filters & Column Visibility
# For quick delivery, store in DB if you have a user_prefs table,
# else fallback to signed cookies/localStorage on the client. Here:
# implement minimal JSON endpoints (store in DB if available).
# -------------------------------------------------------------------
@app.get("/api/prefs/columns")
def get_column_prefs():
    # TODO: replace with real user id
    uid = 1
    cur = get_db().cursor()
    cur.execute("SELECT value FROM user_prefs WHERE user_id=%s AND key='candidate_columns'", (uid,))
    row = cur.fetchone()
    return jsonify({"ok": True, "columns": json.loads(row[0]) if row else None})

@app.post("/api/prefs/columns")
def set_column_prefs():
    uid = 1
    cols = request.json.get("columns", [])
    cur = get_db().cursor()
    cur.execute("""INSERT INTO user_prefs(user_id,key,value)
                   VALUES(%s,'candidate_columns',%s)
                   ON CONFLICT(user_id,key) DO UPDATE SET value=EXCLUDED.value""",
                   (uid, json.dumps(cols)))
    get_db().commit()
    return jsonify({"ok": True})

@app.get("/api/prefs/filters")
def get_saved_filters():
    uid = 1
    cur = get_db().cursor()
    cur.execute("SELECT value FROM user_prefs WHERE user_id=%s AND key='candidate_filters'", (uid,))
    row = cur.fetchone()
    return jsonify({"ok": True, "filters": json.loads(row[0]) if row else []})

@app.post("/api/prefs/filters")
def save_filter():
    uid = 1
    newf = request.json.get("filter")
    cur = get_db().cursor()
    cur.execute("SELECT value FROM user_prefs WHERE user_id=%s AND key='candidate_filters'", (uid,))
    row = cur.fetchone()
    filters = json.loads(row[0]) if row else []
    filters = [f for f in filters if f.get("name") != newf.get("name")]
    filters.append(newf)
    cur.execute("""INSERT INTO user_prefs(user_id,key,value)
                   VALUES(%s,'candidate_filters',%s)
                   ON CONFLICT(user_id,key) DO UPDATE SET value=EXCLUDED.value""",
                   (uid, json.dumps(filters)))
    get_db().commit()
    return jsonify({"ok": True})

@app.delete("/api/prefs/filters")
def delete_filter():
    uid = 1
    name = request.args.get("name")
    cur = get_db().cursor()
    cur.execute("SELECT value FROM user_prefs WHERE user_id=%s AND key='candidate_filters'", (uid,))
    row = cur.fetchone()
    filters = json.loads(row[0]) if row else []
    filters = [f for f in filters if f.get("name") != name]
    cur.execute("""INSERT INTO user_prefs(user_id,key,value)
                   VALUES(%s,'candidate_filters',%s)
                   ON CONFLICT(user_id,key) DO UPDATE SET value=EXCLUDED.value""",
                   (uid, json.dumps(filters)))
    get_db().commit()
    return jsonify({"ok": True})

# PATCH: Point 2 - Column Mapping API
