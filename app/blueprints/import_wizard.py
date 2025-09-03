"""
Domain blueprint: import_wizard
Extracted from main blueprint without changing logic.
"""
from flask import Blueprint
from flask_login import login_required
from ..core import *  # import original helpers and app context
bp = Blueprint("import_wizard", __name__)

@bp.route("/api/import/save", methods=["POST"])
@login_required
def api_import_save():

    data = request.get_json(force=True) or {}
    req_id = data.get("requirement_id")
    rows = data.get("rows", [])
    inserted = len(rows)
    return jsonify({"ok": True, "saved": inserted})


# ---- API response helpers ----
def api_ok(data=None, message='OK', status=200):
    return jsonify({'ok': True, 'message': message, 'data': data}), status

def api_error(message='Error', status=400, errors=None):
    payload = {'ok': False, 'message': message}
    if errors is not None:
        payload['errors'] = errors
    return jsonify(payload), status

@bp.route("/api/import/parse", methods=["POST"])
@csrf_protect
def import_parse():

    file = request.files.get('file')
    text = request.form.get('text')
    if file:
        df = pd.read_excel(file) if file.filename.endswith('.xlsx') else pd.read_csv(file)
    else:
        if not text:
            return jsonify({'ok': False, 'error': 'No file or text provided'}), 400
        rows = [r for r in [line.strip() for line in text.splitlines()] if r]
        cells = [re.split(r'\t|,', r) for r in rows]
        if cells:
            headers = [c.strip() for c in cells[0]]
            data_rows = cells[1:]
        else:
            headers, data_rows = [], []
        df = pd.DataFrame(data_rows, columns=headers) if headers else pd.DataFrame(data_rows)

    system_columns = get_system_columns()
    alias_map = get_header_aliases()

    headers = list(df.columns)
    suggested_mapping = {}
    unmapped_headers = []

    for col in headers:
        alias = col.strip().lower()
        if alias in alias_map:
            suggested_mapping[col] = alias_map[alias]
        else:
            unmapped_headers.append(col)
            suggested_mapping[col] = None

    sample_rows = df.head(5).to_dict(orient='records')

    return jsonify({
        'ok': True,
        'columns': headers,
        'system_fields': system_columns,
        'suggested_mapping': suggested_mapping,
        'unmapped_headers': unmapped_headers,
        'samples': sample_rows
    })

@bp.route("/api/import/validate", methods=["POST"])
@login_required
def api_import_validate():

    data = request.get_json(force=True) or {}
    rows = data.get("rows", [])
    today = datetime.date.today().isoformat()
    errors = []
    normalized = []
    for i, r in enumerate(rows):
        rr = {k: r.get(k, "") for k in SHEET_COLUMNS}
        if not rr.get("candidate_name"):
            errors.append({"row_index": i, "field": "candidate_name", "message": "Candidate name is required"})
        if not rr.get("application_date"):
            rr["application_date"] = today
        normalized.append(rr)
    return jsonify({"ok": True, "rows": normalized, "errors": errors})

@bp.route("/api/import/paste", methods=["POST"])
@csrf_protect
def import_paste():

    data = request.get_json()
    rows = data.get("rows", [])

    parsed_rows = []
    for row in rows:
        normalized = _normalize_row_smart(row)
        errors = _validate_row_smart(normalized)
        parsed_rows.append({"row": normalized, "errors": errors})

    return jsonify({"rows": parsed_rows})
