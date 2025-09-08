# import_routes.py  (MERGED & PATCHED)
import os
import uuid
import json
import pandas as pd
from flask import Blueprint, request, jsonify, session, render_template
from datetime import datetime
import psycopg2
from fuzzywuzzy import process
import re
import unicodedata
from functools import lru_cache
from typing import Optional, Tuple
from flask_login import login_required, current_user
from uuid import UUID

# Optional semantic embeddings (fallback to fuzzy)
try:
    from sentence_transformers import SentenceTransformer
    EMB_MODEL = SentenceTransformer('all-MiniLM-L6-v2')
    def compute_embedding(text):
        return EMB_MODEL.encode([text], convert_to_numpy=True)[0]
    EMB_ENABLED = True
except Exception:
    EMB_ENABLED = False
    def compute_embedding(text):
        return None

# Blueprint
import_bp = Blueprint("import_bp", __name__)

# ✅ Use DATABASE_URL from environment
DATABASE_URL = os.getenv('DATABASE_URL')

# DB config (keep yours)

UPLOAD_DIR = os.path.join(os.path.dirname(__file__), "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

UPLOAD_STORE = {}  # in-memory store for DataFrames (same as before)

# Canonical schema dictionary (editable)
CANONICAL_SCHEMA = {
    "email": ["email", "email id", "emailaddress", "e-mail", "emails", "mail", "contact email"],
    "phone": ["phone", "phone number", "mobile", "mobileno", "cell", "contact no", "phone_number"],
    "current_company": ["company", "current company", "organisation", "organization", "org", "curr company"],
    "education": ["education", "ug degree", "under graduation degree", "alma mater", "qualification"],
    "ctc_current": ["ctc", "current ctc", "ctc_current", "ctc current"],
    "current_ctc_lpa": ["current ctc lpa", "current_ctc_lpa", "ctc lpa", "Ans(What is your current CTC in Lakhs per annum?)"],
    "expected_ctc_lpa": ["expected ctc lpa", "expected_ctc_lpa", "Ans(What is your expected CTC in Lakhs per annum?)"],
    "employee_size": ["employee size", "company size", "team size", "Ans(What is the Employee size of your current company?)"],
    "companies_worked": ["companies worked", "no of companies", "companies_worked", "total companies", "Ans(How many companies you have worked with till now?)"],
    "comments": ["comments", "notes", "remarks"]
}

# --- Forced mappings (grouped by target; highest priority) ---
# Each list is a set of header variants that should *force-map* to the DB column on the left.
FORCED_GROUPS = {
    "application_date": ["applicationdate","applieddate","dateapplied","candapplicationdate"],
    "job_title": ["jobtitle","designation","role","position","title"],
    "candidate_name": ["name","fullname","candidatename","applicant","applicantname"],
    "current_company": ["currentcompany","presentcompany","company","companyname","employer","employername","organisation","organization","org","currcompany","currentemployer","presentemployer"],
    "total_experience": ["experience","totalexperience","overall_experience","yearsofexperience","expyrs","yrsofexp","exp"],
    "phones": ["phonenumber","phone","phoneno","phno","mobile","mobileno","mobile_number","cell","cellphone","contactnumber","contactno","whatsapp","whatsappno","whatsappnumber","telephone","tel"],
    "emails": ["email","emailid","email_id","emailaddress","e-mail","e_mail","mail","workemail","personalemail","primaryemail","secondaryemail","contactemail","businessmail","officialemail","emial","emai"],
    "notice_period": ["noticeperiod","npdays","noticeperioddays","notice_days"],
    "notice_period_details": ["Offer","Offer Details","offer in hand"],
    "current_location": ["location","currentlocation","baselocation","joblocation","officelocation","city","workcity"],
    "preferred_locations": ["preferredlocation","preferredlocations","preferredcity","preferredcities","desiredlocation","desiredlocations","relocationpreference"],
    "ctc_current": ["ctc","presentctc","currentctc","annualctc","ctcyearly","yearlyctc","package","currentpackage","salary","currentsalary"],
    "ectc": ["ectc","expectedctc","expectedsalary","expectedpackage","expectedannualsalary","expectedsalarylpa"],
    "current_ctc_lpa": ["ctclpa","currentctclpa","ctc(lpa)","salarylpa","ctc_lpa"],
    "expected_ctc_lpa": ["expectedctclpa","expctclpa"],
    "key_skills": ["skills","keyskills","primaryskills","secondaryskills","skillset","technologies","techstack","stack"],
    "education": ["education","highestqualification","qualification","degree","ugdegree","undergraduationdegree","undergraduate","bachelors","phd","diploma","alma","alumni","college","school"],
    "post_graduation": ["postgraduate","masters", "Post Graduation Degree", "postgraduation", "post graduation degree"],
    "pf_docs_confirm": ["pf","providentfund","pfdocs","pfconfirmation", "Ans(Do you have all PF and other documents from all previous companies.)"],
    "employee_size": ["employeesize","companysize","teamsize","teamheadcount","orgsize","headcount"],
    "companies_worked": ["companiesworked","companycount","employerscount","pastcompanies","totalcompanies","noofcompanies"],
    "calling_status": ["callingstatus","callstatus","telecallstatus","phonecallstatus"],
    "profile_status": ["profilestatus","status","candidatestatus","applicationstatus"],
    "comments": ["comments","notes","remarks","reviewcomments","recruiternotes","additionalnotes","feedback"],
    "added_date": ["addeddate","createddate"],
    "updated_date": ["updateddate","modifieddate"],
    "added_by": ["addedby","createdby"],
}

# Flatten + normalized forced maps for O(1) lookups by uploaded header
FORCED_MAPPINGS = {alias: target for target, aliases in FORCED_GROUPS.items() for alias in aliases}






def strip_accents(s: str) -> str:
    if not s:
        return ""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in nfkd if not unicodedata.combining(ch))

@lru_cache(maxsize=4096)
def normalize_col(name: str) -> str:
    """
    Robust normalization:
      - unwrap Ans(...) shells
      - lowercase
      - strip accents (é -> e)
      - drop emojis/symbols (keep alnum + separators first then remove seps)
      - collapse separators (_ - . / space)
      - keep only a-z0-9
      - trim trailing numeric suffixes (email1 -> email)
    """
    if not name:
        return ""
    s = str(name).strip()
    if s.lower().startswith("ans(") and s.endswith(")"):
        s = s[4:-1]



    s = strip_accents(s).lower()
    s = "".join(ch if (ch.isalnum() or ch in " _-./") else " " for ch in s)
    for sep in [" ", "_", "-", ".", "/"]:
        s = s.replace(sep, "")
    while s and s[-1].isdigit():
        s = s[:-1]
    return s

# Build normalized forced mapping after normalize_col is available
FORCED_MAPPINGS_NORM = {normalize_col(alias): target for alias, target in FORCED_MAPPINGS.items()}
# Precompute forced keys for nearest-match fuzzy
FORCED_KEYS_RAW = list(FORCED_MAPPINGS.keys())
FORCED_KEYS_NORM = list(FORCED_MAPPINGS_NORM.keys())
# Refresh flattened forced maps after adding new variants
FORCED_MAPPINGS = {alias: target for target, aliases in FORCED_GROUPS.items() for alias in aliases}
FORCED_MAPPINGS_NORM = {normalize_col(alias): target for alias, target in FORCED_MAPPINGS.items()}
FORCED_KEYS_RAW = list(FORCED_MAPPINGS.keys())
FORCED_KEYS_NORM = list(FORCED_MAPPINGS_NORM.keys())


# DB helpers
def get_db_connection():
    return psycopg2.connect(DATABASE_URL)
    return psycopg2.connect(**DB_CONFIG)

def ensure_memory_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS import_mapping_memory (
            id SERIAL PRIMARY KEY,
            uploaded_col_norm TEXT NOT NULL,
            uploaded_col_raw  TEXT,
            db_col            TEXT NOT NULL,
            weight            INTEGER DEFAULT 1,
            confidence        NUMERIC DEFAULT 1.0,
            last_used         TIMESTAMP DEFAULT NOW(),
            UNIQUE (uploaded_col_norm, db_col)
        );
    """)

def load_learned_map(cur):
    cur.execute("SELECT uploaded_col_norm, db_col, weight, confidence FROM import_mapping_memory;")
    rows = cur.fetchall()
    mem = {}
    for norm, dbcol, w, conf in rows:
        mem.setdefault(norm, []).append((dbcol, int(w or 0), float(conf or 1.0)))
    for norm in mem:
        mem[norm].sort(key=lambda t: t[1], reverse=True)
    return mem

# Build canonical lookup for quick exact/alias matches
CANONICAL_LOOKUP = {}
for dbcol, variants in CANONICAL_SCHEMA.items():
    for v in variants:
        CANONICAL_LOOKUP[normalize_col(v)] = dbcol

# Precompute embeddings for canonical names if embeddings enabled
CANONICAL_EMB = {}
if EMB_ENABLED:
    for dbcol, variants in CANONICAL_SCHEMA.items():
        text = dbcol + " " + " ".join(variants)
        emb = compute_embedding(text)
        CANONICAL_EMB[dbcol] = emb

def heuristic_guess(norm: str) -> Tuple[Optional[str], float, str]:
    """
    Conservative token-based heuristics to rescue common variants while
    keeping ambiguous salary/compensation terms for manual review.
    """
    if "email" in norm:
        return "email", 0.92, "Heuristic: token 'email'"
    if any(tok in norm for tok in ["phone", "mobile", "phno", "tele"]):
        return "phone", 0.90, "Heuristic: phone-like token"
    if any(tok in norm for tok in ["org", "company", "employer"]):
        return "current_company", 0.88, "Heuristic: company/employer token"
    if any(tok in norm for tok in ["edu", "degree", "qualification", "almat"]):
        return "education", 0.85, "Heuristic: education token"
    if "comment" in norm or "note" in norm or "remark" in norm:
        return "comments", 0.90, "Heuristic: notes/comments token"
    if any(tok in norm for tok in ["teamsize","companysize","employeesize","headcount"]):
        return "employee_size", 0.85, "Heuristic: size/headcount token"
    if "ctc" in norm and "lpa" in norm:
        if "expect" in norm or "exp" in norm:
            return "expected_ctc_lpa", 0.90, "Heuristic: expected + ctc lpa"
        return "current_ctc_lpa", 0.88, "Heuristic: ctc lpa"
    if "ctc" in norm and any(t in norm for t in ["current","present","now","yr","annual","yearly"]):
        return "ctc_current", 0.86, "Heuristic: ctc current-ish"
    return None, 0.0, "No heuristic"

# Semantic match: returns (dbcol, score, reason)
def semantic_match(uploaded_col, db_columns):
    norm = normalize_col(uploaded_col)
    # 1. check canonical lookup
    if norm in CANONICAL_LOOKUP:
        return CANONICAL_LOOKUP[norm], 1.0, "Canonical dictionary"

    # 3. learned memory (match by normalized uploaded col)
    conn = get_db_connection()
    cur = conn.cursor()
    ensure_memory_table(cur)
    learned = load_learned_map(cur)
    cur.close()
    conn.close()
    if norm in learned and learned[norm]:
        top = learned[norm][0]
        return top[0], min(1.0, top[1] / (top[1] + 1)), "Learned"

    # 3.5. heuristic guess
    dbcol_h, score_h, why_h = heuristic_guess(norm)
    if dbcol_h and dbcol_h in db_columns and dbcol_h != "Not Needed":
        return dbcol_h, float(score_h), why_h

    # 4. semantic/embeddings
    if EMB_ENABLED:
        emb_u = compute_embedding(uploaded_col)
        best = None
        best_score = -1
        for dbc, emb in CANONICAL_EMB.items():
            import numpy as np
            s = float(np.dot(emb_u, emb) / (np.linalg.norm(emb_u) * np.linalg.norm(emb) + 1e-9))
            if s > best_score:
                best_score = s
                best = dbc
        if best_score is not None:
            return best, float(best_score), "Semantic"

    # 5. fallback to fuzzy
    if db_columns:
        res = process.extractOne(uploaded_col, db_columns)
        if res:
            best_match, score = res
            return best_match, float(score/100.0), "Fuzzy"

    return None, 0.0, "No match"

# validate function (keep your schema logic)
def validate_row_against_schema(row, schema):
    errors = []
    for col, col_type in schema.items():
        val = row.get(col)
        if col_type == "integer":
            if val is None or str(val).strip() == "" or not str(val).isdigit():
                errors.append(f"{col} must be an integer")
        elif col_type == "email":
            if not val or not re.match(r"[^@]+@[^@]+\.[^@]+", str(val)):
                errors.append(f"{col} is not a valid email")
        elif col_type == "phone":
            if not val or not str(val).isdigit():
                errors.append(f"{col} must be numeric")
        elif col_type == "required":
            if val is None or str(val).strip() == "":
                errors.append(f"{col} is required")
    return errors

# --- ROUTES ---

@import_bp.route("/candidates/import", methods=["GET"])
@import_bp.route("/candidates/import/<int:req_id>", methods=["GET"])
def import_page(req_id):
    return render_template("candidates_import.html", req_id=req_id)

@import_bp.route("/candidates/import/upload", methods=["POST"])
def upload_candidates():
    if "file" not in request.files:
        return jsonify({"success": False, "error": "No file uploaded"})

    file = request.files["file"]
    filename = file.filename

    try:
        if filename.endswith(".csv"):
            df = pd.read_csv(file)
        else:
            df = pd.read_excel(file)
    except Exception as e:
        return jsonify({"success": False, "error": f"Failed to read file: {str(e)}"})

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""SELECT column_name FROM information_schema.columns WHERE table_name='candidates';""")
    db_columns = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()

    if "Not Needed" not in db_columns:
        db_columns.append("Not Needed")

    mappings = []
    used = set()
    for col in df.columns:
        matched = None
        status = "Not Matched"
        confidence = 0.0
        reason = "None"
        reason_sm = "" 
        norm_col = normalize_col(col)
        # 1) Forced (normalized) first
        if norm_col in FORCED_MAPPINGS_NORM:
            cand = FORCED_MAPPINGS_NORM[norm_col]
            if cand in db_columns and cand != "Not Needed" and cand not in used:
                matched = cand; status = "Matched (Forced: normalized)"; confidence = 1.0; reason = "Forced normalized"
                used.add(cand)
        # 2) Nearest match to forced (fuzzy on normalized forced keys)
        if not matched and FORCED_KEYS_NORM:
            try:
                best_key, best_score = process.extractOne(norm_col, FORCED_KEYS_NORM)
                scoref = (best_score or 0)/100.0
                if scoref >= 0.90:
                    cand = FORCED_MAPPINGS_NORM.get(best_key)
                    if cand in db_columns and cand != "Not Needed" and cand not in used:
                        matched = cand; status = "Matched (Forced: nearest)"; confidence = float(scoref); reason = "Forced nearest"
                        used.add(cand)
            except Exception:
                pass

        if not matched:
            dbcol, score, reason_sm = semantic_match(col, db_columns)
            if score >= 0.98 and dbcol and dbcol not in used:
                matched = dbcol; status = f"Matched ({reason_sm})"; confidence = float(score)
                used.add(dbcol)
            else:
                if dbcol and score > 0.60 and dbcol not in used and dbcol in db_columns and dbcol != "Not Needed":
                    matched = dbcol; status = f"Matched ({reason_sm})"; confidence = float(score)
                    used.add(dbcol)

        if not matched:
            remaining = [c for c in db_columns if c != "Not Needed" and c not in used]
            if remaining:
                res = process.extractOne(col, remaining)
                if res:
                    best_match, score = res
                    scoref = score / 100.0
                    if scoref >= 0.60:
                        if normalize_col(best_match)[:3] == normalize_col(col)[:3] or scoref > 0.80:
                            matched = best_match; status = "Matched (Fuzzy)"; confidence = scoref
                            used.add(best_match)

        mappings.append({
            "uploaded": col,
            "matched": matched,
            "status": status,
            "confidence": round(float(confidence), 3),
            "reason": reason if (matched is None or status.startswith("Matched (Forced")) else (reason_sm or reason)

        })

    upload_id = str(uuid.uuid4())
    UPLOAD_STORE[upload_id] = df
    session["upload_id"] = upload_id
    csv_path = os.path.join(UPLOAD_DIR, f"{upload_id}.csv")
    df.to_csv(csv_path, index=False)

    return jsonify({
        "success": True,
        "upload_id": upload_id,
        "mappings": mappings,
        "db_columns": db_columns,
        "total_rows": len(df)
    })

@import_bp.route("/candidates/import/validate", methods=["POST"])
def validate_candidates():
    data = request.get_json(silent=True) or {}
    mapping = data.get("mapping")
    schema = data.get("schema", {})

    if mapping:
        if isinstance(mapping, list):
            session["mapping"] = {m["uploaded"]: m["matched"] for m in mapping if m.get("matched") and m["matched"] != "Not Needed"}
        elif isinstance(mapping, dict):
            session["mapping"] = {k: v for k, v in mapping.items() if v and v != "Not Needed"}

    upload_id = data.get("upload_id")
    if not upload_id or upload_id not in UPLOAD_STORE:
        return jsonify({"success": False, "error": "Upload session expired. Please re-upload file."})

    df = UPLOAD_STORE[upload_id]
    validated = []
    for idx, row in df.iterrows():
        rowdict = row.to_dict()
        errors = validate_row_against_schema(rowdict, schema)
        validated.append({
            "rownum": idx + 1,
            "data": rowdict,
            "status": "error" if errors else "ok",
            "error": "; ".join(errors) if errors else ""
        })

    return jsonify({
        "success": True,
        "upload_id": upload_id,
        "columns": list(df.columns),
        "rows": validated
    })

@import_bp.route("/candidates/import/commit", methods=["POST"])
def commit_candidates():
    import json
    from uuid import UUID

    # --- Accept both JSON (validation modal) and form (review page) ---
    payload = request.get_json(silent=True) or {}
    is_json = bool(payload)

    if is_json:
        requirement_id = payload.get("requirement_id")
        mapping_raw = payload.get("mapping")
        rows_json = payload.get("rows")
        # rows_json from validation modal: [{rownum, data:{...}, ...}]
        if isinstance(rows_json, list) and rows_json and isinstance(rows_json[0], dict) and "data" in rows_json[0]:
            rows = [r.get("data", {}) for r in rows_json]
        else:
            rows = rows_json if isinstance(rows_json, list) else []
    else:
        requirement_id = request.form.get("requirement_id")
        edited_data = request.form.get("edited_data")
        try:
            rows = json.loads(edited_data or "[]")
        except Exception:
            return jsonify({"status": "error", "message": "Invalid JSON data"}), 400
        mapping_raw = request.form.get("mapping")

    # --- Validate requirement_id ---
    if not requirement_id or not str(requirement_id).isdigit():
        return jsonify({"status": "error", "message": "Missing or invalid requirement_id"}), 400
    requirement_id = int(requirement_id)

    # --- Parse mapping (array or dict); ignore "Not Needed" ---
    mapping = None
    if mapping_raw:
        try:
            mapping = mapping_raw if isinstance(mapping_raw, dict) else json.loads(mapping_raw)
        except Exception:
            mapping = None
    if isinstance(mapping, list):
        mapping = {m.get("uploaded"): m.get("matched")
                   for m in mapping if m.get("uploaded") and m.get("matched") and m.get("matched") != "Not Needed"}
    if not mapping or not isinstance(mapping, dict):
        return jsonify({"status": "error", "message": "Mapping not found or invalid format in POST data"}), 400

    # --- Open DB + introspect columns + added_by type ---
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""SELECT column_name, data_type, udt_name
                       FROM information_schema.columns
                       WHERE table_name='candidates';""")
        cols_meta = cur.fetchall()
        db_columns = {r[0] for r in cols_meta}
        added_by_type = None
        for col_name, data_type, udt_name in cols_meta:
            if col_name == "added_by":
                # Postgres reports UUID as data_type 'uuid' or udt_name 'uuid'
                added_by_type = (data_type or "").lower() or (udt_name or "").lower()
                break

        # --- Derive current user id (don’t require login decorator) ---
        user_id_raw = None
        try:
            # flask-login style
            from flask_login import current_user
            if current_user is not None:
                user_id_raw = getattr(current_user, "id", None)
                if user_id_raw is None and hasattr(current_user, "get_id"):
                    user_id_raw = current_user.get_id()
        except Exception:
            pass
        # Also allow session fallback
        if user_id_raw is None:
            user_id_raw = (session.get("user_id") or session.get("uid") or None)

        # Coerce based on DB column type (if present)
        def coerce_added_by(val):
            if val is None or "added_by" not in db_columns:
                return None
            # UUID column?
            if added_by_type == "uuid":
                try:
                    return str(UUID(str(val)))
                except Exception:
                    return None
            # Integer-like column?
            if added_by_type in {"integer", "int4", "bigint", "int8", "smallint", "int2"}:
                try:
                    return int(val)
                except Exception:
                    return None
            # Anything else -> store as text
            return str(val)

        added_by_value = session.get("username") 

        # --- Insert rows ---
        inserted = 0
        for row in (rows or []):
            mapped_row = {}
            for uploaded_col, db_col in mapping.items():
                if db_col and db_col != "Not Needed" and db_col in db_columns:
                    mapped_row[db_col] = row.get(uploaded_col)

            # skip totally empty rows
            if not any(v and str(v).strip() for v in mapped_row.values()):
                continue

            # system fields
            mapped_row["application_date"] = datetime.now()
            mapped_row["requirement_id"] = requirement_id

            # stamp added_by if we could coerce it
            if added_by_value is not None:
                mapped_row["added_by"] = added_by_value

            cols = list(mapped_row.keys())
            vals = list(mapped_row.values())
            placeholders = ", ".join(["%s"] * len(vals))
            colnames = ", ".join([f'"{c}"' for c in cols])
            query = f"INSERT INTO candidates ({colnames}) VALUES ({placeholders})"
            cur.execute(query, vals)
            inserted += 1

        conn.commit()
    except Exception as e:
        conn.rollback()
        # Return the first ~300 chars so you can see the actual DB cause
        return jsonify({"status": "error", "message": f"Commit failed: {str(e)[:300]}"}), 500
    finally:
        cur.close()
        conn.close()

    # --- Persist mapping memory (unchanged from your code) ---
    try:
        conn2 = get_db_connection()
        cur2 = conn2.cursor()
        ensure_memory_table(cur2)
        for uploaded_col, db_col in mapping.items():
            if db_col and db_col != "Not Needed":
                cur2.execute("""
                    INSERT INTO import_mapping_memory (uploaded_col_norm, uploaded_col_raw, db_col, weight, confidence, last_used)
                    VALUES (%s, %s, %s, 1, %s, NOW())
                    ON CONFLICT (uploaded_col_norm, db_col)
                    DO UPDATE SET weight = import_mapping_memory.weight + 1,
                                  confidence = LEAST(1.0, COALESCE(import_mapping_memory.confidence, 1.0) + %s),
                                  last_used = NOW();
                """, (normalize_col(uploaded_col), uploaded_col, db_col, 0.1, 0.1))
        conn2.commit()
        cur2.close()
        conn2.close()
    except Exception as e:
        print("Warning: failed to persist mapping memory:", str(e))

    return jsonify({"status": "ok", "rows_inserted": inserted})

@import_bp.route("/candidates/import/mapping/remember", methods=["POST"])
def remember_mapping_now():
    """
    Persist user-provided mapping pairs immediately from the mapping UI,
    so next uploads auto-map even if the user doesn't complete a full import.
    Expected JSON: {"pairs": [{"uploaded": "...", "matched": "..."}, ...]}
    """
    data = request.get_json(silent=True) or {}
    pairs = data.get("pairs") or []

    if not isinstance(pairs, list) or not pairs:
        return jsonify({"success": False, "error": "No mapping pairs supplied"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    ensure_memory_table(cur)

    upserts = 0
    for p in pairs:
        u = (p.get("uploaded") or "").strip()
        m = (p.get("matched") or "").strip()
        if not u or not m or m == "Not Needed":
            continue
        cur.execute("""
            INSERT INTO import_mapping_memory (uploaded_col_norm, uploaded_col_raw, db_col, weight, confidence, last_used)
            VALUES (%s, %s, %s, 1, %s, NOW())
            ON CONFLICT (uploaded_col_norm, db_col)
            DO UPDATE SET weight = import_mapping_memory.weight + 1,
                          confidence = LEAST(1.0, COALESCE(import_mapping_memory.confidence, 1.0) + %s),
                          last_used = NOW();
        """, (normalize_col(u), u, m, 0.2, 0.2))
        upserts += 1

    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"success": True, "upserts": upserts})

@import_bp.route("/admin/import_mappings", methods=["GET"])
def admin_import_mappings():
    conn = get_db_connection()
    cur = conn.cursor()
    ensure_memory_table(cur)
    cur.execute("SELECT uploaded_col_norm, uploaded_col_raw, db_col, weight, confidence, last_used FROM import_mapping_memory ORDER BY weight DESC;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    mappings = []
    for r in rows:
        mappings.append({
            "uploaded_col_norm": r[0],
            "uploaded_col_raw": r[1],
            "db_col": r[2],
            "weight": r[3],
            "confidence": float(r[4] or 0),
            "last_used": r[5].isoformat() if r[5] else None
        })
    return jsonify({"mappings": mappings})









# --- Restored routes from backup ---

MANDATORY_FIELDS = ["Name", "Email ID", "Phone Number"]

@import_bp.route("/review/<upload_id>")
def candidate_review(upload_id):
    df = None
    if upload_id in UPLOAD_STORE:
        df = UPLOAD_STORE[upload_id]
    else:
        csv_path = os.path.join(UPLOAD_DIR, f"{upload_id}.csv")
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)

    if df is None:
        return "Upload not found", 404

    mapping = session.get("mapping", {})
    if mapping:
        df = df.rename(columns={ucol: dbcol for ucol, dbcol in mapping.items() if dbcol and dbcol != "Not Needed"})

    dup_emails, dup_phones = set(), set()
    if "Email" in df.columns:
        dup_emails = set(df["Email"].astype(str).str.strip().value_counts()[lambda x: x > 1].index)
    if "Phone" in df.columns:
        dup_phones = set(df["Phone"].astype(str).str.strip().value_counts()[lambda x: x > 1].index)

    rows = []
    for _, row in df.iterrows():
        rowdict = row.to_dict()
        errors = []

        for f in MANDATORY_FIELDS:
            if f in rowdict:
                val = rowdict.get(f)
                if val is None or str(val).strip() == "" or str(val).lower() == "nan":
                    errors.append(f"{f} is required")

        if "Email" in rowdict and str(rowdict["Email"]).strip() in dup_emails:
            errors.append("Duplicate Email")
        if "Phone" in rowdict and str(rowdict["Phone"]).strip() in dup_phones:
            errors.append("Duplicate Phone")

        rows.append({
            "data": rowdict,
            "valid": len(errors) == 0,
            "errors": errors
        })

    req_id = session.get("requirement_id") or request.args.get("requirement_id") or request.args.get("req_id")
    try:
        req_id = int(req_id) if req_id is not None and str(req_id).isdigit() else None
    except Exception:
        req_id = None

    return render_template(
        "candidate_review.html",
        upload_id=upload_id,
        rows=rows,
        columns=list(df.columns),
        req_id=req_id
    )


@import_bp.route("/candidates/import/mapping_summary", methods=["POST"])
def mapping_summary():
    data = request.get_json()
    upload_id = data.get("upload_id")
    mapping = data.get("mapping", [])

    if not upload_id or upload_id not in UPLOAD_STORE:
        return jsonify({"success": False, "error": "Upload session expired. Please re-upload file."})

    df = UPLOAD_STORE[upload_id]
    total_rows = len(df)
    matched_count = sum(1 for m in mapping if m.get("matched") and m.get("matched") != "Not Needed")
    not_needed_count = sum(1 for m in mapping if m.get("matched") == "Not Needed")

    return jsonify({
        "success": True,
        "total_rows": total_rows,
        "matched_count": matched_count,
        "not_needed_count": not_needed_count
    })


@import_bp.route("/candidates/import/list_uploads", methods=["GET"])
def list_uploads():
    files = []
    for fname in os.listdir(UPLOAD_DIR):
        if fname.endswith(".csv") or fname.endswith(".xlsx"):
            upload_id = os.path.splitext(fname)[0]
            files.append(upload_id)
    return jsonify({"uploads": files})


@import_bp.route("/commit", methods=["POST"])
def commit_candidates_base():
    # Delegate to the already defined commit handler
    return commit_candidates()
