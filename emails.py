# emails.py
import time
import json
import html as _html
from datetime import datetime

# kept for compatibility if Flask-Mail is available
try:
    from flask_mail import Message
except Exception:
    Message = None

# -----------------------
# Helper & existing functions (kept / lightly refactored)
# -----------------------

def parse_assigned_csv(csv_text):
    """Return normalized list of usernames from an assigned_to CSV string."""
    if not csv_text:
        return []
    return [u.strip() for u in csv_text.split(',') if u and u.strip()]

def _fetch_emails_for_usernames(cur, usernames):
    """Return dict username -> email for given usernames (only entries with non-empty email)."""
    if not usernames:
        return {}
    cur.execute("SELECT username, email FROM users WHERE username = ANY(%s)", (usernames,))
    rows = cur.fetchall()
    # support dict-like or sequence rows
    out = {}
    for r in rows:
        if isinstance(r, dict):
            uname = r.get('username')
            em = r.get('email')
        else:
            # assume tuple (username, email)
            try:
                uname = r[0]
                em = r[1]
            except Exception:
                uname = None
                em = None
        if uname and em:
            out[uname] = em
    return out

def _send_mail_with_retry(subject, plain_body, html_body, to_emails, max_retries=3, sender_name=None, cc_emails=None, use_bcc=True):
    """Send mail via Flask-Mail with retry/backoff.
    If use_bcc is True (default), messages are sent with To=<internal sender> and BCC=recipients.
    If use_bcc is False, recipients are placed in the To: header directly (one-to-one style).

    sender_name: optional display name to include in From header (e.g. "John Smith").
    cc_emails: optional list of email addresses to CC (e.g. logged-in user's email).
    """
    try:
        from main import app, EMAIL_USER, mail
    except Exception:
        app = None
        EMAIL_USER = None
        mail = None

    if not to_emails:
        if app:
            app.logger.info("_send_mail_with_retry: no recipients, skipping")
        return False

    uniq_emails = list(dict.fromkeys(to_emails))

    default_sender = None
    try:
        default_sender = app.config.get('MAIL_DEFAULT_SENDER') if app else None
    except Exception:
        default_sender = None

    if isinstance(default_sender, (list, tuple)):
        sender_email = default_sender[1] if len(default_sender) > 1 else default_sender[0]
    else:
        sender_email = default_sender or EMAIL_USER or "no-reply@example.com"

    # Build display name + email for message sender. Use provided sender_name if present.
    if sender_name:
        # standard display: "Full Name <email@domain>"
        sender_formatted = f"{sender_name} <{sender_email}>"
    else:
        sender_formatted = f"ATS Portal <{sender_email}>"

    # prepare cc list if present
    cc_list = None
    if cc_emails:
        if isinstance(cc_emails, (list, tuple)):
            cc_list = list(dict.fromkeys([e for e in cc_emails if e]))
        elif isinstance(cc_emails, str) and cc_emails.strip():
            cc_list = [cc_emails.strip()]
        else:
            cc_list = None

    for attempt in range(1, max_retries + 1):
        try:
            if mail is None:
                # mail not configured — log and return True to avoid breaking flows in non-prod
                if app:
                    app.logger.info("_send_mail_with_retry: mail not configured; skipping actual send for %s", uniq_emails)
                return True

            # prepare Message parameters
            if use_bcc:
                msg_kwargs = dict(
                    subject=subject,
                    recipients=[sender_email],  # To header (internal)
                    bcc=uniq_emails,
                    body=plain_body,
                    html=html_body,
                    sender=sender_formatted
                )
            else:
                # one-to-one behavior: recipients in To header
                msg_kwargs = dict(
                    subject=subject,
                    recipients=uniq_emails,
                    body=plain_body,
                    html=html_body,
                    sender=sender_formatted
                )

            # include cc if provided
            if cc_list:
                msg_kwargs['cc'] = cc_list

            # If Flask-Mail Message class or mail instance is unavailable, simulate send in dev
            if Message is None or mail is None:
                if app:
                    app.logger.info("_send_mail_with_retry: Message or mail not available; simulating send to %s", uniq_emails)
                return True

            msg = Message(**msg_kwargs)
            mail.send(msg)
            if app:
                app.logger.info("_send_mail_with_retry: sent '%s' (recipients=%s bcc=%s cc=%s)", subject, msg_kwargs.get('recipients'), msg_kwargs.get('bcc'), msg_kwargs.get('cc'))
            return True
        except Exception:
            if app:
                app.logger.exception("_send_mail_with_retry: attempt %s failed", attempt)
            time.sleep(0.5 * attempt)

    if app:
        app.logger.error("_send_mail_with_retry: all attempts failed for %s", uniq_emails)
    return False

def _escape(s):
    return __html.escape('' if s is None else str(s))

def _shorten(text, limit=350):
    if not text:
        return ''
    t = text.strip()
    if len(t) <= limit:
        return t
    cut = t.rfind(' ', 0, limit)
    if cut <= 0:
        cut = limit
    return t[:cut].rstrip() + '...'

# -----------------------
# Requirement notification (unchanged)
# -----------------------

def send_requirement_email(requirement, target_usernames, action='created', cur=None):
    """Send a polished HTML + plain-text requirement notification."""
    try:
        from main import app, get_db_cursor
    except Exception:
        app = None

    if not target_usernames:
        if app:
            app.logger.info("send_requirement_email: no target_usernames provided, skipping")
        return

    target_usernames = [u for u in (target_usernames or []) if u and u.strip()]
    if not target_usernames:
        if app:
            app.logger.info("send_requirement_email: targets empty after normalization, skipping")
        return

    title = requirement.get('requirement_name') or '(no title)'
    if action == 'created':
        subject = f"New Role Assigned: {title}"
        intro_line = "A new role has been assigned to you."
    else:
        subject = f"Role Updated: {title}"
        intro_line = "A role assigned to you has been updated."

    # Fields to display
    fields_ordered = [
        ('Hiring Company', requirement.get('client_name')),
        ('Client POC', requirement.get('client_poc')),
        ('End Client', requirement.get('end_client')),
        ('Hiring Manager', requirement.get('hiring_manager')),
        ('Location', requirement.get('job_locations')),
        ('Work Mode', ('Remote' if requirement.get('remote') else requirement.get('work_mode'))
            if (requirement.get('remote') is not None or requirement.get('work_mode')) else None),
        ('Experience', requirement.get('experience')),
        ('Openings', requirement.get('openings')),
        ('Notice Period', requirement.get('notice_period')),
        ('Compensation', requirement.get('budget')),
        ('Job Type', requirement.get('job_type')),
        ('Employment Type', requirement.get('employment_type')),
        ('Qualifications', requirement.get('qualifications')),
        ('Notice Period (Months)', requirement.get('notice_period_months')),
        ('Interview Process', requirement.get('interview_process')),
        ('Bond', requirement.get('bond')),
        ('Status', requirement.get('status')),
        ('Currency', requirement.get('budget_currency')),
        ('Location Details', requirement.get('location_details')),
    ]

    # Skills
    skills = [s.strip() for s in _html.unescape(requirement.get('mandatory_skills') or '')
              .replace(';', ',').split(',') if s.strip()]
    secondary_skills = [s.strip() for s in _html.unescape(requirement.get('secondary_skills') or '')
                        .replace(';', ',').split(',') if s.strip()]

    responsibilities = requirement.get('responsibilities') or requirement.get('role_responsibilities')
    benefits = requirement.get('benefits')
    description = requirement.get('job_description')

    # Fixed CTA link
    job_link = "https://reqtool-app.onrender.com/requirements?status=Active"

    # Rows HTML
    rows_html = ''.join(
        f"<tr><td style='padding:8px 10px;width:40%;font-weight:600;color:#0b1220'>{_escape(label)}</td>"
        f"<td style='padding:8px 10px;color:#0f172a'>{_escape(val)}</td></tr>"
        for label, val in fields_ordered if val
    )

    def badge_html(s):
        return f'<span style="display:inline-block;background:#eef6ff;color:#0b1220;padding:6px 10px;border-radius:14px;margin:4px 6px;font-size:13px">{_escape(s)}</span>'
    skills_html = ''.join(badge_html(s) for s in skills)
    secondary_html = ''.join(badge_html(s) for s in secondary_skills)

    html_body = f"""<!doctype html>
<html>
  <head>
    <meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
  </head>
  <body style="margin:0;padding:18px;background:#f7fafc;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
    <div style="max-width:760px;margin:0 auto;">
      <div style="background:#ffffff;border-radius:10px;padding:22px;border:1px solid #e6eef8;">
        <h2 style="margin:6px 0 14px 0;color:#0b1220;font-size:20px">{_escape(title)}</h2>
        <p style="margin:6px 0 14px 0;color:#334155">{_escape(intro_line)}</p>

        <div style="display:flex;gap:18px;flex-wrap:wrap;">
          <div style="flex:1;min-width:260px;">
            <table style="width:100%;font-size:14px;color:#0f172a;border-collapse:collapse;">
              <tbody>{rows_html}</tbody>
            </table>
          </div>

          <div style="flex:1;min-width:260px;">
            <div style="margin-bottom:12px">
              <strong style="display:block;margin-bottom:8px">Key Skills</strong>
              <div>{skills_html or '<span style=color:#94a3b8>—</span>'}</div>
              {('<div style="margin-top:8px"><strong>Secondary Skills</strong><div>'+secondary_html+'</div></div>') if secondary_html else ''}
            </div>

            <div style="margin-top:8px">
              <strong style="display:block;margin-bottom:8px">Description</strong>
              <div style="color:#334155;line-height:1.5">{_escape(_shorten(description,700)) if description else '<span style=color:#94a3b8>—</span>'}</div>
            </div>

            {('<div style="margin-top:12px"><strong>Responsibilities</strong><div style="color:#334155;line-height:1.45">'+_escape(_shorten(responsibilities,400))+'</div></div>') if responsibilities else ''}
            {('<div style="margin-top:12px"><strong>Benefits</strong><div style="color:#334155;line-height:1.45">'+_escape(_shorten(benefits,300))+'</div></div>') if benefits else ''}
          </div>
        </div>

        <div style="text-align:center;margin-top:18px">
          <a href="{job_link}" style="background:#0ea5e9;color:#fff;padding:11px 18px;border-radius:8px;text-decoration:none;font-weight:700;display:inline-block">🔎 View Role in Portal</a>
        </div>
      </div>
    </div>
  </body>
</html>"""

    bullets = "\n".join(f" - {s}" for s in skills) if skills else " - —"
    fields_txt = "\n".join(f"{k}: {v}" for k,v in fields_ordered if v)
    plain_body = f"""{intro_line}

Job Title: {title}
{fields_txt}

Key Skills:
{bullets}

Description:
{(description or '—')[:1000]}

View in Portal: {job_link}
"""

    try:
        if cur is None:
            with get_db_cursor() as (_conn, _cur):
                emails_map = _fetch_emails_for_usernames(_cur, target_usernames)
        else:
            emails_map = _fetch_emails_for_usernames(cur, target_usernames)
    except Exception:
        if app:
            app.logger.exception("send_requirement_email: failed to fetch emails")
        return

    emails = [emails_map[u] for u in target_usernames if emails_map.get(u)]
    if not emails:
        if app:
            app.logger.warning("send_requirement_email: no email addresses for %s", target_usernames)
        return

    sent_ok = _send_mail_with_retry(subject, plain_body, html_body, emails)
    if not sent_ok and app:
        app.logger.error("send_requirement_email: failed to send notification for %s", emails)


# ---- ADDED: send_jd_to_candidates (kept intact, but calls use_bcc=False) ----

def send_jd_to_candidates(requirement, candidates, subject_template=None, body_html_template=None, body_text_template=None, initiator_user_id=None, persist_audit=False, cur=None):
    """
    Robust JD sending helper that does NOT require Flask-Mail to be installed.
    Uses internal _send_mail_with_retry() as the primary sending mechanism which safely
    handles the case where mail is unconfigured (it will log and return True).
    Returns a report dict: {total, sent, failed, skipped, details: [...]}

    Note: optional `cur` param used to resolve initiator_user_id -> email via DB when necessary.
    """
    try:
        from flask import current_app as app
    except Exception:
        app = None

    # attempt to resolve recruiter email/display from requirement if available
    recruiter_email = None
    try:
        recruiter_email = requirement.get('owner_email') or requirement.get('recruiter_email') or requirement.get('added_by_email')
    except Exception:
        recruiter_email = None

    import re, json, html as _html
    report = {"total": len(candidates or []), "sent": 0, "failed": 0, "skipped": 0, "details": []}

    # Resolve initiator email (the logged-in user who started the send)
    initiator_email = None
    try:
        # If a raw email string was provided as initiator_user_id, use it
        if isinstance(initiator_user_id, str) and "@" in initiator_user_id:
            initiator_email = initiator_user_id.strip()
        else:
            # Try session (common pattern in your code)
            try:
                from main import session
                if session and isinstance(session, dict):
                    se = session.get('email') or session.get('user_email') or session.get('user')
                    if se and isinstance(se, str) and "@" in se:
                        initiator_email = se.strip()
            except Exception:
                pass

            # If we still don't have it, and a DB cursor is available, attempt to fetch using numeric id or username
            if not initiator_email and cur is not None and initiator_user_id:
                try:
                    # if initiator_user_id looks numeric, try id; otherwise try username
                    if isinstance(initiator_user_id, int) or (isinstance(initiator_user_id, str) and initiator_user_id.isdigit()):
                        cur.execute("SELECT email FROM users WHERE id = %s", (int(initiator_user_id),))
                    else:
                        cur.execute("SELECT email FROM users WHERE username = %s", (initiator_user_id,))
                    row = cur.fetchone()
                    if row:
                        if isinstance(row, dict):
                            maybe = row.get('email')
                        else:
                            maybe = row[0] if len(row) > 0 else None
                        if maybe and isinstance(maybe, str) and "@" in maybe:
                            initiator_email = maybe.strip()
                except Exception:
                    if app:
                        app.logger.exception("send_jd_to_candidates: failed to resolve initiator_user_id email")
                    # fall through silently
    except Exception:
        initiator_email = None

    if app:
        app.logger.info("send_jd_to_candidates: resolved initiator_email=%s recruiter_email=%s", initiator_email, recruiter_email)

    # simple token function
    def render_template(tpl, cand, req):
        if not tpl:
            return ''
        out = tpl
        try:
            first_name = (cand.get('candidate_name') or '').strip().split()[0] if cand.get('candidate_name') else ''
            out = out.replace('{{first_name}}', first_name)
            out = out.replace('{{candidate_name}}', cand.get('candidate_name') or '')
            out = out.replace('{{requirement_name}}', (req.get('requirement_name') if isinstance(req, dict) else '') or '')
            out = out.replace('{{client_name}}', (req.get('client_name') if isinstance(req, dict) else '') or '')
        except Exception:
            pass
        return out

    # iterate candidates
    for idx, cand in enumerate(candidates or []):
        cand_id = cand.get('id') or None
        raw_emails = cand.get('primary_email') or cand.get('primary') or cand.get('emails') or ''
        emails_list = []
        if isinstance(raw_emails, (list, tuple)):
            emails_list = [str(x).strip() for x in raw_emails if x and str(x).strip()]
        elif isinstance(raw_emails, str):
            s = raw_emails.strip()
            if not s:
                emails_list = []
            else:
                try:
                    parsed = json.loads(s)
                    if isinstance(parsed, (list, tuple)):
                        emails_list = [str(x).strip() for x in parsed if x and str(x).strip()]
                    else:
                        emails_list = [s]
                except Exception:
                    emails_list = [e.strip() for e in s.split(',') if e.strip()]
        else:
            emails_list = []

        primary_email = emails_list[0] if emails_list else None
        if not primary_email:
            report['skipped'] += 1
            report['details'].append({'candidate_id': cand_id, 'email': None, 'status': 'skipped', 'error': 'no email'})
            continue

        # Prepare subject/body with token replacement
        subj = render_template(subject_template or 'Job opportunity', cand, requirement)
        plain_body = render_template(body_text_template or ('We have a job: {{requirement_name}}'), cand, requirement)
        html_body = render_template(body_html_template or (plain_body.replace('\n','<br/>') if plain_body else ''), cand, requirement)

        # Build cc list: include recruiter_email and initiator_email (deduped)
        cc_list = []
        if recruiter_email:
            cc_list.append(recruiter_email)
        if initiator_email:
            cc_list.append(initiator_email)
        # normalize/unique
        cc_final = None
        cc_sanitized = [e.strip() for e in cc_list if e and isinstance(e, str) and e.strip()]
        if cc_sanitized:
            # dedupe preserving order
            seen_cc = {}
            cc_final = []
            for e in cc_sanitized:
                le = e.lower()
                if le not in seen_cc:
                    seen_cc[le] = True
                    cc_final.append(e)

        # Attempt to send using internal retry helper. This handles "mail not configured" gracefully.
        try:
            ok = _send_mail_with_retry(
                subj,
                plain_body,
                html_body,
                [primary_email],
                sender_name="Recruitment Team",
                cc_emails=cc_final,
                use_bcc=False
            )
            if ok:
                report['sent'] += 1
                report['details'].append({'candidate_id': cand_id, 'email': primary_email, 'status': 'sent'})
            else:
                report['failed'] += 1
                report['details'].append({'candidate_id': cand_id, 'email': primary_email, 'status': 'failed', 'error': 'mail helper failed'})
        except Exception as e:
            if app:
                app.logger.exception("send_jd_to_candidates: failed for %s", primary_email)
            report['failed'] += 1
            report['details'].append({'candidate_id': cand_id, 'email': primary_email, 'status': 'failed', 'error': str(e)})

    return report

# ---- end send_jd_to_candidates ----


# ---- NEW / REFRACTORED HELPERS FOR INTERVIEW EMAIL ----

def _normalize_emails_field(raw):
    """Return list of emails from list/tuple, JSON string, or comma-separated string."""
    if not raw:
        return []
    if isinstance(raw, (list, tuple)):
        return [e for e in raw if e]
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, (list, tuple)):
                return [str(e).strip() for e in parsed if e and str(e).strip()]
        except Exception:
            # not JSON — fall back to comma split
            return [e.strip() for e in raw.split(',') if e.strip()]
    # unknown format
    return []


def _format_skill_list(raw_skills):
    try:
        s = _html.unescape(raw_skills or '')
        s = s.replace(';', ',')
        parts = [p.strip() for p in s.split(',') if p.strip()]
        return ", ".join(parts)
    except Exception:
        return raw_skills or ''


def _build_interview_email_payload(requirement, candidate, recruiter_display, interview_date, interview_time, ist_display, is_reschedule=False):
    """
    Build (subject, plain_body, html_body) matching the requested template.
    """
    client_name = requirement.get('client_name') or ''
    client_link = requirement.get('client_linkedin_profile') or requirement.get('client_link') or ''
    requirement_name = requirement.get('requirement_name') or requirement.get('title') or ''
    experience = requirement.get('experience') or ''
    mandatory_skills = _format_skill_list(requirement.get('mandatory_skills') or '')
    job_location = requirement.get('job_locations') or ('Remote' if requirement.get('remote') else '')
    role_highlights = requirement.get('job_description') or ''

    cand_name = candidate.get('candidate_name') or ''
    job_title = requirement_name or candidate.get('job_title') or ''

    subject = f"Interview Confirmation – {job_title} at {client_name}" if client_name else f"Interview Confirmation – {job_title}"

    supersede_note = ""
    supersede_html = ""
    if is_reschedule:
        supersede_note = "NOTE: This email supersedes any previous interview confirmation. Please disregard earlier schedules.\n\n"
        supersede_html = '<p style="background:#fff3cd;padding:8px;border-radius:6px;border:1px solid #ffeeba;color:#664d03">This email supersedes any previous interview confirmation. Please disregard earlier schedules.</p>'

    # Plain text (user template)
    plain_body = f"""Subject: {subject}

Dear {cand_name or 'Candidate'},

{supersede_note}As per your discussion with {recruiter_display}, we are pleased to confirm your interview for the {job_title} role with {client_name or 'the client'}. Please find the details below:

Position Details

Company Name: {client_name or '—'}
LinkedIn: {client_link or '—'}
Requirement: {requirement_name or job_title}
Experience Required: {experience or '—'}
Mandatory Skills: {mandatory_skills or '—'}
Job Location: {job_location or '—'}

Role Highlights:
{(role_highlights or '—')[:1500]}

Interview Schedule

Date: {interview_date or '—'}
Time: {interview_time or '—'} ({ist_display or '—'})

You will shortly receive a separate email with the official interview invite and joining details.

Kindly confirm your availability for the above schedule. Should you need any adjustments, please let us know at the earliest.

We look forward to your participation and wish you success in the process.

Best Regards,
{recruiter_display}
"""

    # HTML body (polished)
    safe = _html.escape
    link_html = f'<a href="{safe(client_link)}">{safe(client_link)}</a>' if client_link else '—'
    html_body = f"""<!doctype html>
<html><body style="font-family:Arial,Helvetica,sans-serif;color:#0b1220">
  <div style="max-width:720px;margin:0 auto;padding:12px">
    <h2 style="margin-bottom:6px">Interview Confirmation</h2>
    <p>Dear {safe(cand_name or 'Candidate')},</p>
    {supersede_html}
    <p>As per your discussion with <strong>{safe(recruiter_display)}</strong>, we are pleased to confirm your interview for the <strong>{safe(job_title)}</strong> role with <strong>{safe(client_name or 'the client')}</strong>. Please find the details below:</p>

    <h3 style="margin-bottom:6px">Position Details</h3>
    <ul>
      <li><strong>Company Name:</strong> {safe(client_name or '—')}</li>
      <li><strong>LinkedIn:</strong> {link_html}</li>
      <li><strong>Requirement:</strong> {safe(requirement_name or job_title)}</li>
      <li><strong>Experience Required:</strong> {safe(experience or '—')}</li>
      <li><strong>Mandatory Skills:</strong> {safe(mandatory_skills or '—')}</li>
      <li><strong>Job Location:</strong> {safe(job_location or '—')}</li>
    </ul>

    <h3 style="margin-bottom:6px">Role Highlights</h3>
    <div style="color:#334155;line-height:1.4">{safe((role_highlights or '—')[:2000])}</div>

    <h3 style="margin-bottom:6px">Interview Schedule</h3>
    <p><strong>Date:</strong> {safe(interview_date or '—')}<br/>
       <strong>Time:</strong> {safe(interview_time or '—')} — <em>{safe(ist_display or '')}</em></p>

    <p>You will shortly receive a separate email with the official interview invite and joining details.</p>
    <p><strong>Kindly confirm your availability</strong> for the above schedule. Should you need any adjustments, please let us know at the earliest.</p>

    <p>We look forward to your participation and wish you success in the process.</p>

    <p>Best regards,<br/>{safe(recruiter_display)}</p>
  </div>
</body></html>"""
    return subject, plain_body, html_body

# ---- NEW: robust send_candidate_interview_email (with is_reschedule flag) ----

def send_candidate_interview_email(candidate, requirement=None, interview_date=None, interview_time=None, cur=None, is_reschedule=False):
    """
    Send a complete Interview Confirmation email.

    - If `requirement` is a partial dict or just {'id':...}, and `cur` is provided, the full requirement row
      is re-fetched to obtain client_name, client_linkedin_profile, mandatory_skills, experience, job_locations, remote, job_description.
    - Attempts to resolve recruiter_display from session (main.session -> users table first_name/last_name), falls back to candidates.added_by if necessary.
    - Expects interview_date in YYYY-MM-DD and interview_time in HH:MM (assumed IST).
    - is_reschedule: if True, a supersede note is included in the message (same confirmation template).
    - Returns a dict report: {'total': n, 'sent': n_sent, 'failed': n_failed, 'details': [...]}
    """
    import logging
    from datetime import datetime, timedelta, timezone

    # Try to import app and session for logging / current user
    try:
        from main import app, session
    except Exception:
        app = None
        session = {}

    # normalize candidate emails
    raw_emails = candidate.get('emails') if candidate else None
    emails = _normalize_emails_field(raw_emails)

    if not emails:
        if app:
            app.logger.warning("send_candidate_interview_email: no recipient emails for candidate %s", candidate.get('candidate_name'))
        return {'total': 0, 'sent': 0, 'failed': 0, 'details': [{'candidate': candidate.get('id'), 'reason': 'no emails'}]}

    # Ensure we have full requirement dict
    req = dict(requirement) if isinstance(requirement, dict) else {}
    # if requirement provided as id or missing some fields, try to fetch from DB
    if cur is not None:
        req_id = req.get('id') or req.get('requirement_id') or candidate.get('requirement_id')
        if req_id:
            try:
                cur.execute(
                    "SELECT id, requirement_name, client_name, client_poc, client_linkedin_profile, mandatory_skills, experience, job_locations, remote, job_description "
                    "FROM requirements WHERE id = %s",
                    (req_id,)
                )
                fres = cur.fetchone()
                if fres:
                    # merge fields; handle dict-like or sequence result
                    try:
                        req.update(dict(fres))
                    except Exception:
                        try:
                            req.update(fres)
                        except Exception:
                            if app:
                                app.logger.warning("send_candidate_interview_email: fetched requirement but could not merge fields cleanly for id %s", req_id)
            except Exception:
                if app:
                    app.logger.exception("send_candidate_interview_email: failed to fetch requirement id %s", req_id)

    # -----------------------
    # Recruiter resolution (use actual users schema: first_name + last_name)
    # -----------------------
    recruiter_display = None
    recruiter_email = None
    try:
        username = session.get('username') if session else None
    except Exception:
        username = None

    if username and cur is not None:
        try:
            # select known columns your app uses: first_name, last_name, email, username
            cur.execute("SELECT first_name, last_name, email, username FROM users WHERE username = %s", (username,))
            urow = cur.fetchone()
            if urow:
                if isinstance(urow, dict):
                    fn = (urow.get('first_name') or '').strip()
                    ln = (urow.get('last_name') or '').strip()
                    em = urow.get('email')
                    usr = urow.get('username') or username
                else:
                    vals = list(urow)
                    fn = (vals[0] or '').strip() if len(vals) > 0 else ''
                    ln = (vals[1] or '').strip() if len(vals) > 1 else ''
                    em = vals[2] if len(vals) > 2 else None
                    usr = vals[3] if len(vals) > 3 else username

                combined = " ".join(p for p in (fn, ln) if p).strip()
                if combined:
                    recruiter_display = combined
                elif em:
                    recruiter_display = em
                else:
                    recruiter_display = usr or username

                # capture recruiter email separately for CC
                if em:
                    recruiter_email = em
        except Exception:
            if app:
                app.logger.exception("send_candidate_interview_email: failed to fetch user details for username %s", username)
            recruiter_display = username

    # fallback to candidates.added_by if recruiter not resolved
    if not recruiter_display and cur is not None:
        try:
            cand_id = candidate.get('id')
            if cand_id:
                cur.execute("SELECT added_by FROM candidates WHERE id = %s", (cand_id,))
                crow = cur.fetchone()
                if crow:
                    if isinstance(crow, dict):
                        recruiter_display = crow.get('added_by')
                    else:
                        recruiter_display = (crow[0] if len(crow) > 0 else None)
        except Exception:
            if app:
                app.logger.exception("send_candidate_interview_email: failed to fetch added_by for candidate %s", candidate.get('id'))

    if not recruiter_display:
        recruiter_display = username or "Recruiter"

    # Build display datetime in IST
    try:
        from zoneinfo import ZoneInfo
        ist_tz = ZoneInfo("Asia/Kolkata")
    except Exception:
        ist_tz = timezone(timedelta(hours=5, minutes=30))

    ist_display = ""
    if interview_date and interview_time:
        try:
            dt_naive = datetime.strptime(f"{interview_date} {interview_time}", "%Y-%m-%d %H:%M")
            dt_aware = dt_naive.replace(tzinfo=ist_tz)
            tz_offset = dt_aware.utcoffset()
            offset_str = ""
            if tz_offset is not None:
                total_min = int(tz_offset.total_seconds() // 60)
                sign = "+" if total_min >= 0 else "-"
                hh = abs(total_min) // 60
                mm = abs(total_min) % 60
                offset_str = f"UTC{sign}{hh:02d}:{mm:02d}"
            ist_display = f"{dt_aware.strftime('%b %d, %Y at %I:%M %p')} IST ({offset_str})"
        except Exception:
            ist_display = f"{interview_date} {interview_time} IST"

    # Build payload (passes is_reschedule so payload adds supersede note)
    subject, plain_body, html_body = _build_interview_email_payload(req, candidate, recruiter_display, interview_date, interview_time, ist_display, is_reschedule=is_reschedule)

    # try to send via helper
    report = {'total': len(emails), 'sent': 0, 'failed': 0, 'details': []}
    try:
        ok = _send_mail_with_retry(subject, plain_body, html_body, emails, sender_name=recruiter_display, cc_emails=( [recruiter_email] if recruiter_email else None) )
        if ok:
            report['sent'] = len(emails)
            report['details'].append({'recipients': emails, 'status': 'sent'})
            if app:
                app.logger.info("send_candidate_interview_email: sent interview confirmation to %s for candidate %s (reschedule=%s)", emails, candidate.get('candidate_name'), is_reschedule)
        else:
            report['failed'] = len(emails)
            report['details'].append({'recipients': emails, 'status': 'failed', 'error': 'mail send helper returned False'})
            if app:
                app.logger.error("send_candidate_interview_email: mail helper reported failure for %s", emails)
    except Exception as e:
        report['failed'] = len(emails)
        report['details'].append({'recipients': emails, 'status': 'failed', 'error': str(e)})
        if app:
            app.logger.exception("send_candidate_interview_email: exception sending to %s", emails)

    return report

# End of file



# === Helper: Render requirement JD without sending ===


def render_requirement_jd(requirement):
    """
    Render a polished, professional HTML and plain-text JD email.
    Adjustments made:
      - Mandatory skill badges use a green theme (pill-shaped).
      - Location & Experience card is placed in the right column and aligned
        directly under the "View Company" button (left-aligned under the button
        so it shares the same left edge/alignment as the company button).
    """
    import html as _html
    from datetime import datetime as _dt

    # safe getter
    def g(k):
        if not isinstance(requirement, dict):
            return ""
        v = requirement.get(k)
        return "" if v is None else v

    client = g("client_name") or g("client") or ""
    title = g("requirement_name") or g("title") or ""
    experience = g("experience") or ""
    mandatory_skills = g("mandatory_skills") or ""
    job_locations = g("job_locations") or ""
    remote = g("remote")
    remote_text = "Remote" if (remote in [True, "True", "true", "1", 1]) else ""
    location_display = job_locations or remote_text or ""
    job_description = g("job_description") or g("description") or ""
    client_link = g("client_linkedin_profile") or g("client_link") or ""
    client_brief = g("client_brief_description") or g("client_brief") or ""
    req_id = requirement.get("id") if isinstance(requirement, dict) else ""

    subject = f"{title} at {client}" if client else (title or "Job Opportunity")
    now_str = _dt.utcnow().strftime("%b %d, %Y")

    # normalize skills into list
    def skills_list(s):
        if not s:
            return []
        if isinstance(s, (list, tuple)):
            return [str(x).strip() for x in s if x and str(x).strip()]
        s = str(s)
        parts = [p.strip() for p in s.replace(";", ",").split(",") if p.strip()]
        return parts

    skills = skills_list(mandatory_skills)

    # create badges for skills (email-safe inline styles) - green theme
    def render_skill_badges(skills_list):
        if not skills_list:
            return '<div style="font-size:14px;color:#6b7280;">No skills specified</div>'
        badges = []
        for s in skills_list:
            # green pill badge
            badges.append(
                '<span style="display:inline-block;padding:6px 12px;margin:6px 6px;border-radius:999px;'
                'background:#e6f9ec;color:#065f46;font-size:13px;border:1px solid #c6f0d1;">'
                + _html.escape(s) + "</span>"
            )
        return '<div style="line-height:1.4;">' + "".join(badges) + '</div>'

    skills_html = render_skill_badges(skills)

    # LinkedIn / View Company button (inline SVG for broad client support)
    def linkedin_button(url):
        if not url:
            return ""
        svg = (
            '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" '
            'xmlns="http://www.w3.org/2000/svg" style="vertical-align:middle;margin-right:8px;">'
            '<path d="M4.98 3.5C4.98 4.88 3.87 6 2.5 6C1.12 6 0 4.88 0 3.5C0 2.12 1.12 1 2.5 1C3.87 1 4.98 2.12 4.98 3.5Z" fill="#fff"/>'
            '<path d="M0 8.5H5V24H0V8.5Z" fill="#fff"/>'
            '<path d="M8 8.5H13V10.6C13.69 9.4 15.36 8.5 17.5 8.5C21.5 8.5 24 10.8 24 15.5V24H19V16.5C19 14 18 13 16 13C14 13 13.5 14.3 13.5 16V24H8V8.5Z" fill="#fff"/>'
            "</svg>"
        )
        return (
            f'<a href="{_html.escape(url)}" '
            'style="display:inline-block;padding:10px 14px;border-radius:8px;background:#0A66C2;color:#fff;'
            'text-decoration:none;font-weight:700;font-size:14px;margin-top:8px;">'
            f'{svg}View Company</a>'
        )

    linkedin_html = linkedin_button(client_link)

    # Build the HTML. Use table layout for best email-client support.
    # Important change: Use a single-cell sub-table under the company/button and align that cell LEFT.
    full_html = (
        '<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"/>'
        f'<title>{_html.escape(subject)}</title></head>'
        '<body style="margin:0;background:#f5f7fb;font-family:Segoe UI, Roboto, Arial, sans-serif;">'
        '<table width="100%" cellpadding="0" cellspacing="0" role="presentation">'
        '<tr><td align="center" style="padding:28px 12px;">'
        '<table width="700" cellpadding="0" cellspacing="0" role="presentation" style="max-width:700px;background:#ffffff;border-radius:10px;overflow:hidden;border:1px solid #e6edf3;">'
        # header
        '<tr><td style="background:linear-gradient(90deg,#0ea5a3 0%,#6366f1 100%);padding:22px 24px;color:#fff;">'
        '<table width="100%"><tr>'
        # company on left and date on right
        f'<td style="font-size:18px;font-weight:800;">{_html.escape(client or "Hiring Team")}</td>'
        f'<td align="right" style="font-size:13px;opacity:0.95;">{_html.escape(now_str)}</td>'
        "</tr>"
        # title (preserve same size and weight as before)
        f'<tr><td colspan="2" style="padding-top:8px;font-size:24px;font-weight:900;letter-spacing:0.2px;">{_html.escape(title)}</td></tr>'
        "</table></td></tr>"
        # main two-column block
        '<tr><td style="padding:8px 24px 18px 24px;">'
        '<table width="100%" cellpadding="0" cellspacing="0" role="presentation"><tr>'
        # left: role summary
        '<td style="vertical-align:top;padding-right:14px;width:60%;">'
        '<h4 style="margin:0 0 10px 0;font-size:16px;color:#0f1724;">Role Summary</h4>'
        f'<div style="padding:12px;border-radius:8px;border:1px solid #eef2f6;background:#fff;color:#111827;line-height:1.6;font-size:14px;">{_html.escape(job_description)}</div>'
        "</td>"
        # right: skills + company + view company + (Location & Experience positioned directly under the button, left-aligned)
        '<td style="vertical-align:top;padding-left:14px;width:40%;">'
        '<h4 style="margin:0 0 10px 0;font-size:16px;color:#0f1724;">Mandatory Skills</h4>'
        f'<div style="padding:12px;border-radius:8px;border:1px solid #eef2f6;background:#fff;">{skills_html}</div>'
        '<div style="margin-top:14px;font-size:14px;color:#243242;">'
        '<div style="font-weight:800;margin-bottom:6px;color:#0f1724;font-size:16px">Company</div>'
        # company name bigger and darker
        f'<div style="color:#0b1220;margin-bottom:8px;font-size:15px;font-weight:700;">{_html.escape(client)}</div>'
        f'{linkedin_html}'
        # LEFT-aligned sub-table: places the location/experience card directly below the button, matching its left edge
        '<table width="100%" cellpadding="0" cellspacing="0" role="presentation" style="margin-top:12px;"><tr>'
        '<td align="left" style="vertical-align:top;">'
        f'<div style="display:inline-block;padding:12px;border-radius:8px;border:1px solid #eef2f6;background:#fff;color:#0f1724;font-size:14px;text-align:left;">'
        f'<div style="font-weight:700;font-size:13px;margin-bottom:6px;color:#0f1724;">Location</div>'
        f'<div style="font-size:14px;color:#0b1220;margin-bottom:8px;">{_html.escape(location_display or "—")}</div>'
        f'<div style="font-weight:700;font-size:13px;margin-bottom:6px;color:#0f1724;">Experience</div>'
        f'<div style="font-size:14px;color:#0b1220;">{_html.escape(str(experience) or "—")}</div>'
        '</div>'
        '</td></tr></table>'
        "</div></td>"
        "</tr></table></td></tr>"
        # about client
        '<tr><td style="padding:0 24px 18px 24px;">'
        '<div style="padding:14px;border-radius:8px;border:1px solid #eef2f6;background:linear-gradient(180deg,#fff,#fbfdff);font-size:14px;color:#243b55;line-height:1.6;">'
        '<div style="font-weight:700;margin-bottom:8px;color:#0f1724;">About the Client</div>'
        f'<div>{_html.escape(client_brief)}</div>'
        "</div></td></tr>"
        '<tr><td style="padding:18px 24px 24px 24px;font-size:13px;color:#6b7280;">'
        "</td></tr>"
        "</table></td></tr></table></body></html>"
    )

    # plain text fallback (skills as comma-separated, company link as profile)
    text_lines = [
        f"Job Title: {title}",
        f"Client: {client}",
        f"Location: {location_display}",
        f"Experience: {experience}",
        "",
        "Summary:",
        job_description or "",
        "",
        "Mandatory Skills:",
        ", ".join(skills) if skills else "",
        "",
        "About the Client:",
        client_brief or "",
        (f"Company Profile: {client_link}" if client_link else ""),
    ]
    plain_text = "\n".join([str(x) for x in text_lines if x is not None])

    return {"subject": subject, "html": full_html, "text": plain_text}


def send_requirement_jd(requirement, candidates, initiator_user_id=None,
                        subject=None, body_html=None, body_text=None, **kwargs):
    """Render JD templates from requirement if templates not provided,
    then call send_jd_to_candidates.
    Returns whatever send_jd_to_candidates returns.
    """
    templates = render_requirement_jd(requirement)
    subject = subject or templates.get('subject')
    body_html = body_html or templates.get('html')
    body_text = body_text or templates.get('text')

    # call existing function if present
    if 'send_jd_to_candidates' in globals():
        # pass cur through if provided in kwargs
        cur = kwargs.get('cur')
        return send_jd_to_candidates(
            requirement=requirement,
            candidates=candidates,
            subject_template=subject,
            body_html_template=body_html,
            body_text_template=body_text,
            initiator_user_id=initiator_user_id,
            persist_audit=True,
            cur=cur,
            **kwargs
        )
    else:
        raise RuntimeError('send_jd_to_candidates function not found in emails.py')


def _build_jd_email_payload(requirement, candidate, recruiter_display='', recruiter_email=''):
    """
    Build (subject, plain_body, html_body) using the same visual layout as interview emails
    but tailored for Job Description (JD) email (no interview schedule).
    """
    client_name = requirement.get('client_name') or ''
    client_link = requirement.get('client_linkedin_profile') or requirement.get('client_link') or ''
    requirement_name = requirement.get('requirement_name') or requirement.get('title') or ''
    experience = requirement.get('experience') or ''
    mandatory_skills = _format_skill_list(requirement.get('mandatory_skills') or '')
    job_location = requirement.get('job_locations') or ('Remote' if requirement.get('remote') else '')
    role_highlights = requirement.get('job_description') or ''

    cand_name = candidate.get('candidate_name') or ''
    job_title = requirement_name or candidate.get('job_title') or ''

    subject = f"Job Description – {job_title} at {client_name}" if client_name else f"Job Description – {job_title}"

    # plain text body
    plain_body = f"""Subject: {subject}

Dear {cand_name or 'Candidate'},

{recruiter_display} has shared the following job details with you. Please review and reply if you're interested.

Position Details
Company Name: {client_name or '—'}
Company LinkedIn: {client_link or '—'}
Role / Requirement: {requirement_name or job_title}
Experience Required: {experience or '—'}
Location: {job_location or '—'}
Mandatory Skills: {mandatory_skills or '—'}

Role Highlights / Description:
{role_highlights or '—'}

If this role interests you, please reply to this email or contact {recruiter_display or recruiter_email} for next steps.

Regards,
{recruiter_display or 'Recruitment Team'}
"""

    # simple HTML body re-using structure from interview template but without schedule
    html_body = f"""
<html>
  <body style="font-family: Arial, sans-serif; line-height:1.4; color:#111;">
    <div style="max-width:700px; margin:0 auto; padding:16px;">
      <h2 style="margin-bottom:8px;">{job_title or 'Job Opportunity'}</h2>
      <p style="margin-top:0; color:#555;">{client_name or ''} — {job_location or ''}</p>
      <hr/>
      <h3>Role Details</h3>
      <p><strong>Experience:</strong> {experience or '—'}</p>
      <p><strong>Mandatory Skills:</strong> {mandatory_skills or '—'}</p>
      <h3>Role Highlights</h3>
      <div style="white-space:pre-wrap;">{role_highlights or '—'}</div>
      <hr/>
      <p>If you're interested, please reply to this email or click the button below to confirm your interest.</p>
      <p><a href="mailto:{recruiter_email or ''}" style="display:inline-block;padding:10px 16px;border-radius:6px;text-decoration:none;border:1px solid #0b61a4;">Contact Recruiter</a></p>
      <p style="margin-top:24px;">Regards,<br/>{recruiter_display or 'Recruitment Team'}</p>
    </div>
  </body>
</html>
"""
    return subject, plain_body, html_body


def send_jd_using_interview_style(requirement, candidates, cur=None, initiator_user_id=None, persist_audit=False):
    """
    Send JD email to candidates using the interview email visual style (but without schedule).
    requirement: dict (may contain id)
    candidates: list of dicts each containing at least 'primary_email' and 'candidate_name' (and optional 'id')
    Returns a report dict with send results.
    """
    report = {"total": 0, "sent": 0, "failed": 0, "details": []}
    if not candidates:
        return report

    # Ensure requirement is a full dict by refetching if possible
    if isinstance(requirement, dict) and requirement.get('id') and cur is not None and (not requirement.get('client_name')):
        try:
            cur.execute("SELECT * FROM requirements WHERE id = %s", (requirement.get('id'),))
            row = cur.fetchone()
            if row:
                try:
                    cols = [d.name if hasattr(d, 'name') else d[0] for d in cur.description]
                    requirement = dict(zip(cols, row))
                except Exception:
                    try:
                        requirement = dict(row)
                    except Exception:
                        pass
        except Exception:
            pass

    # resolve recruiter display & email similar to interview function
    recruiter_display = ''
    recruiter_email = None
    try:
        # attempt to get from requirement owner fields
        recruiter_email = requirement.get('owner_email') or requirement.get('recruiter_email') or requirement.get('added_by_email')
        recruiter_display = requirement.get('owner_name') or requirement.get('recruiter_name') or (recruiter_email or '').split('@')[0]
    except Exception:
        recruiter_display = ''

    # Resolve initiator email similar to send_jd_to_candidates
    initiator_email = None
    try:
        if isinstance(initiator_user_id, str) and "@" in initiator_user_id:
            initiator_email = initiator_user_id.strip()
        else:
            try:
                from main import session
                if session and isinstance(session, dict):
                    se = session.get('email') or session.get('user_email') or session.get('user')
                    if se and isinstance(se, str) and "@" in se:
                        initiator_email = se.strip()
            except Exception:
                pass
            if not initiator_email and cur is not None and initiator_user_id:
                try:
                    if isinstance(initiator_user_id, int) or (isinstance(initiator_user_id, str) and initiator_user_id.isdigit()):
                        cur.execute("SELECT email FROM users WHERE id = %s", (int(initiator_user_id),))
                    else:
                        cur.execute("SELECT email FROM users WHERE username = %s", (initiator_user_id,))
                    crow = cur.fetchone()
                    if crow:
                        if isinstance(crow, dict):
                            maybe = crow.get('email')
                        else:
                            maybe = crow[0] if len(crow) > 0 else None
                        if maybe and isinstance(maybe, str) and "@" in maybe:
                            initiator_email = maybe.strip()
                except Exception:
                    pass
    except Exception:
        initiator_email = None

    try:
        # For each candidate, build payload and send
        for c in candidates:
            report['total'] += 1
            to_email = (c.get('primary_email') or '').strip()
            if not to_email:
                report['failed'] += 1
                report['details'].append({'candidate': c, 'status': 'no_email'})
                continue

            subject, plain_body, html_body = _build_jd_email_payload(requirement, c, recruiter_display=recruiter_display, recruiter_email=recruiter_email)

            # Build cc list for this candidate send
            cc_list = []
            if recruiter_email:
                cc_list.append(recruiter_email)
            if initiator_email:
                cc_list.append(initiator_email)

            # normalize cc_list => cc_final (dedupe)
            cc_final = None
            cc_sanitized = [e.strip() for e in cc_list if e and isinstance(e, str) and e.strip()]
            if cc_sanitized:
                seen_cc = {}
                cc_final = []
                for e in cc_sanitized:
                    le = e.lower()
                    if le not in seen_cc:
                        seen_cc[le] = True
                        cc_final.append(e)

            try:
                ok = _send_mail_with_retry(
                    subject,
                    plain_body,
                    html_body,
                    [to_email],
                    sender_name="Recruitment Team",
                    cc_emails=cc_final
                )
                if ok:
                    report['sent'] += 1
                    report['details'].append({'candidate': c, 'status': 'sent'})
                    if persist_audit and isinstance(cur, object):
                        try:
                            cur.execute("INSERT INTO email_audit (candidate_id, requirement_id, to_email, subject, status, created_at, sent_by) VALUES (%s,%s,%s,%s,%s,NOW(),%s)",
                                        (c.get('id'), requirement.get('id'), to_email, subject, 'sent', initiator_user_id))
                            # commit is left to caller
                        except Exception:
                            pass
                else:
                    report['failed'] += 1
                    report['details'].append({'candidate': c, 'status': 'failed'})
            except Exception as e:
                report['failed'] += 1
                report['details'].append({'candidate': c, 'status': 'error', 'error': str(e)[:400]})
    except Exception:
        if app:
            app.logger.exception("send_jd_using_interview_style: unexpected error while sending JDs")

    return report
