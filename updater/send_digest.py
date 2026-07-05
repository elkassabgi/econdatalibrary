"""Daily operator digest for the Aqueduct updater — the econ twin of the
hfdatalibrary morning email.

Runs as the LAST step of updater-daily.yml (even when earlier steps failed:
`if: always()`), reads the run's state.db off the runner, and emails a short
honest summary via Resend: per-source status, data frontiers, and anything
needing attention. Sender reuses the Resend-verified hfdatalibrary.com domain.

Honesty rules: the digest reports what the ledger says — counts and dates come
from unit_state rows written only after verified publishes; a red overall run
status is stated in the subject line, never softened. If RESEND_API_KEY is
absent the step prints a loud notice and exits 0 (email is an add-on, not a
gate — the health gate step is what turns runs red).
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import urllib.request

FROM = "Econ Data Library <noreply@hfdatalibrary.com>"
TO = "admin@hfdatalibrary.com"
STATE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                     "data", "_aqueduct", "state.db")


def main() -> None:
    key = os.environ.get("RESEND_API_KEY", "").strip()
    run_status = os.environ.get("RUN_STATUS", "unknown")   # ${{ job.status }} from the yml
    run_id = os.environ.get("GITHUB_RUN_ID", "local")

    con = sqlite3.connect(f"file:{STATE}?mode=ro", uri=True)
    rows = con.execute(
        "SELECT source_id, status, last_obs_date, last_success_utc, last_error "
        "FROM unit_state ORDER BY source_id").fetchall()
    con.close()

    ok = [r for r in rows if r[1] in ("ok", "no_change")]
    warn = [r for r in rows if r[1] in ("partial", "transient_fail")]
    bad = [r for r in rows if r[1] not in ("ok", "no_change", "partial", "transient_fail")]

    healthy = run_status.lower() == "success" and not bad
    subject = (f"econdatalibrary daily: OK — {len(ok)} sources current"
               if healthy else
               f"econdatalibrary daily: ATTENTION — run={run_status}, "
               f"{len(bad)} failed, {len(warn)} retrying")

    feed = "https://econdl-api.elkassabgi.workers.dev/v1/last-updates"
    runlog = f"https://github.com/elkassabgi/econdatalibrary/actions/runs/{run_id}"

    # ---- plain-text fallback (some clients / previews) ----
    lines = [f"Run {run_id}: {run_status}",
             f"{len(ok)} ok/no_change · {len(warn)} partial/transient · {len(bad)} failed", ""]
    for r in sorted(warn + bad, key=lambda x: x[0]):
        lines.append(f"  !! {r[0]:20} {r[1]:15} last_obs={r[2] or '—'}  err={str(r[4] or '')[:90]}")
    if warn or bad:
        lines.append("")
    for r in ok:
        lines.append(f"     {r[0]:20} {r[1]:15} data through {r[2] or '—'}")
    lines += ["", f"Live freshness feed: {feed}", f"Run log: {runlog}"]
    body = chr(10).join(lines)

    # ---- HTML (the real email) ----
    import html as H
    def esc(x): return H.escape(str(x if x is not None else "—"))
    ink, gold = "#0b1220", "#c9a961"
    green, amber, red, grey = "#15803d", "#b45309", "#b91c1c", "#6b7280"

    def chip(n, label, color):
        return (f'<td style="padding:10px 16px;background:#fff;border:1px solid #e5e7eb;'
                f'border-radius:10px;text-align:center;">'
                f'<div style="font-size:22px;font-weight:700;color:{color};'
                f'font-family:Georgia,serif;">{n}</div>'
                f'<div style="font-size:11px;color:{grey};text-transform:uppercase;'
                f'letter-spacing:.05em;">{label}</div></td>')

    attention_rows = ""
    for r in sorted(warn + bad, key=lambda x: x[0]):
        color = red if r in bad else amber
        attention_rows += (
            f'<tr><td style="padding:7px 10px;border-top:1px solid #e5e7eb;'
            f'font-family:Consolas,monospace;font-size:13px;"><b>{esc(r[0])}</b></td>'
            f'<td style="padding:7px 10px;border-top:1px solid #e5e7eb;color:{color};'
            f'font-weight:600;font-size:13px;">{esc(r[1])}</td>'
            f'<td style="padding:7px 10px;border-top:1px solid #e5e7eb;'
            f'font-family:Consolas,monospace;font-size:13px;">{esc(r[2])}</td>'
            f'<td style="padding:7px 10px;border-top:1px solid #e5e7eb;color:{grey};'
            f'font-size:12px;">{esc(str(r[4] or "")[:110])}</td></tr>')

    current_rows = ""
    for r in ok:
        st_color = green if r[1] == "ok" else grey
        current_rows += (
            f'<tr><td style="padding:5px 10px;border-top:1px solid #f3f4f6;'
            f'font-family:Consolas,monospace;font-size:12.5px;">{esc(r[0])}</td>'
            f'<td style="padding:5px 10px;border-top:1px solid #f3f4f6;color:{st_color};'
            f'font-size:12.5px;">{esc(r[1])}</td>'
            f'<td style="padding:5px 10px;border-top:1px solid #f3f4f6;'
            f'font-family:Consolas,monospace;font-size:12.5px;">{esc(r[2])}</td></tr>')

    banner_color = green if healthy else red
    banner_word = "All systems normal" if healthy else "Needs attention"
    html_doc = f"""<!doctype html><html><body style="margin:0;padding:0;background:#f4f5f7;">
<div style="max-width:680px;margin:0 auto;padding:18px;font-family:-apple-system,'Segoe UI',Roboto,Arial,sans-serif;color:#111827;">
  <div style="background:{ink};border-radius:12px 12px 0 0;padding:18px 22px;">
    <div style="color:{gold};font-size:11px;text-transform:uppercase;letter-spacing:.14em;">ElkassabgiData</div>
    <div style="color:#fff;font-family:Georgia,serif;font-size:20px;margin-top:2px;">Econ Data Library — daily report</div>
  </div>
  <div style="background:{banner_color};color:#fff;padding:8px 22px;font-size:13px;font-weight:600;">
    {banner_word} · run {esc(run_id)} · {esc(run_status)}
  </div>
  <div style="background:#fff;border:1px solid #e5e7eb;border-top:none;border-radius:0 0 12px 12px;padding:20px 22px;">
    <table role="presentation" cellspacing="8" style="border-collapse:separate;margin:0 auto 14px;"><tr>
      {chip(len(ok), "current", green)}{chip(len(warn), "retrying", amber)}{chip(len(bad), "failed", red)}
    </tr></table>
    {"<h3 style='font-family:Georgia,serif;font-size:15px;margin:14px 0 6px;color:" + red + ";'>Needs attention</h3><table role='presentation' width='100%' style='border-collapse:collapse;background:#fffbeb;border:1px solid #fde68a;border-radius:8px;'><tr><th align='left' style='padding:7px 10px;font-size:11px;color:" + grey + ";text-transform:uppercase;'>Source</th><th align='left' style='padding:7px 10px;font-size:11px;color:" + grey + ";text-transform:uppercase;'>Status</th><th align='left' style='padding:7px 10px;font-size:11px;color:" + grey + ";text-transform:uppercase;'>Data through</th><th align='left' style='padding:7px 10px;font-size:11px;color:" + grey + ";text-transform:uppercase;'>Detail</th></tr>" + attention_rows + "</table>" if (warn or bad) else ""}
    <h3 style="font-family:Georgia,serif;font-size:15px;margin:18px 0 6px;">Current sources ({len(ok)})</h3>
    <table role="presentation" width="100%" style="border-collapse:collapse;">
      <tr><th align="left" style="padding:5px 10px;font-size:11px;color:{grey};text-transform:uppercase;">Source</th>
          <th align="left" style="padding:5px 10px;font-size:11px;color:{grey};text-transform:uppercase;">Status</th>
          <th align="left" style="padding:5px 10px;font-size:11px;color:{grey};text-transform:uppercase;">Data through</th></tr>
      {current_rows}
    </table>
    <p style="margin:18px 0 0;font-size:12.5px;color:{grey};line-height:1.6;">
      <a href="{feed}" style="color:#1d4ed8;">Live freshness feed</a> ·
      <a href="{runlog}" style="color:#1d4ed8;">Run log</a><br>
      Dates only advance when observations were actually fetched — far-future dates are
      legitimate forecast datasets (e.g. WEO projections). Failures show here until fixed, never hidden.
    </p>
  </div>
</div>
</body></html>"""

    print(f"[digest] {subject}\n{body}\n", flush=True)
    if not key:
        print("[digest] RESEND_API_KEY not set — email SKIPPED (add the GitHub secret "
              "to enable the morning email; the run's red/green state still notifies "
              "via GitHub Actions).", flush=True)
        return

    req = urllib.request.Request(
        "https://api.resend.com/emails",
        data=json.dumps({"from": FROM, "to": [TO], "subject": subject,
                         "text": body, "html": html_doc}).encode(),
        headers={"Authorization": f"Bearer {key}",
                 "Content-Type": "application/json",
                 # api.resend.com sits behind Cloudflare bot protection, which
                 # 1010-blocks urllib's default signature — identify honestly.
                 "User-Agent": "econdatalibrary-digest/1.0"},
        method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            print(f"[digest] sent: HTTP {resp.status}", flush=True)
    except urllib.error.HTTPError as e:
        # Print Resend's exact error body (safe: their error JSON carries no
        # secrets) so a misconfigured key/domain is diagnosable from the log.
        try:
            detail = e.read().decode()[:300]
        except Exception:
            detail = "(no body)"
        print(f"[digest] SEND FAILED: HTTP {e.code} — {detail} (run status is "
              f"still governed by the health gate, not the email)", flush=True)
    except Exception as e:  # noqa: BLE001 — email failure must not flip a green run red
        print(f"[digest] SEND FAILED: {e!r} (run status is still governed by the "
              f"health gate, not the email)", flush=True)


if __name__ == "__main__":
    sys.exit(main())
