from __future__ import annotations

import json
import subprocess
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Iterable


def _date_key(value: str | None) -> str:
    if not value:
        return "undated"
    return value[:10]


def export_jsonl(conn, out_dir: Path) -> dict[str, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    outputs: dict[str, str] = {}
    for table in ("raw_items", "mail_messages", "calendar_events", "contacts"):
        path = out_dir / f"{table}.jsonl"
        with path.open("w", encoding="utf-8") as fh:
            for row in conn.execute(f"SELECT * FROM {table} ORDER BY id"):
                fh.write(json.dumps(dict(row), ensure_ascii=False) + "\n")
        outputs[table] = str(path)
    return outputs


def export_markdown(conn, out_dir: Path) -> dict[str, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    mail_dir = out_dir / "mail"
    cal_dir = out_dir / "calendar"
    contact_dir = out_dir / "contacts"
    for path in (mail_dir, cal_dir, contact_dir):
        path.mkdir(parents=True, exist_ok=True)

    mail_by_day: dict[str, list[dict]] = defaultdict(list)
    for row in conn.execute("SELECT * FROM mail_messages WHERE deleted=0 ORDER BY received_at DESC"):
        item = dict(row)
        mail_by_day[_date_key(item.get("received_at"))].append(item)
    for day, rows in mail_by_day.items():
        lines = [f"# Microsoft Mail — {day}", ""]
        for row in rows:
            sender = row.get("sender_name") or row.get("sender_email") or "Unknown"
            subject = row.get("subject") or "(no subject)"
            link = row.get("web_link")
            source = f"[Open in Outlook]({link})" if link else "Outlook"
            lines.append(f"- **{subject}** — {sender} — {row.get('received_at') or ''} — {source}")
            preview = (row.get("body_preview") or "").strip()
            if preview:
                lines.append(f"  - {preview}")
        (mail_dir / f"{day}.md").write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")

    events_by_day: dict[str, list[dict]] = defaultdict(list)
    for row in conn.execute("SELECT * FROM calendar_events WHERE deleted=0 AND coalesce(is_cancelled,0)=0 ORDER BY start_at"):
        item = dict(row)
        events_by_day[_date_key(item.get("start_at"))].append(item)
    for day, rows in events_by_day.items():
        lines = [f"# Microsoft Calendar — {day}", ""]
        for row in rows:
            subject = row.get("subject") or "(busy)"
            start = row.get("start_at") or ""
            end = row.get("end_at") or ""
            org = row.get("organizer_name") or row.get("organizer_email") or "Unknown organizer"
            loc = f" @ {row['location']}" if row.get("location") else ""
            link = row.get("web_link")
            source = f"[Open in Outlook]({link})" if link else "Outlook"
            lines.append(f"- **{subject}** {start} - {end}{loc} — organizer: {org} — {source}")
        (cal_dir / f"{day}.md").write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")

    contacts = [dict(row) for row in conn.execute("SELECT * FROM contacts WHERE deleted=0 ORDER BY display_name")]
    lines = ["# Microsoft Contacts", ""]
    for row in contacts:
        emails = ", ".join((e.get("address") or e.get("name") or "") for e in json.loads(row.get("email_addresses") or "[]"))
        company = f" — {row['company_name']}" if row.get("company_name") else ""
        title = f", {row['job_title']}" if row.get("job_title") else ""
        lines.append(f"- **{row.get('display_name') or '(unnamed)'}**{company}{title} — {emails}")
    (contact_dir / "contacts.md").write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")

    return {"mail": str(mail_dir), "calendar": str(cal_dir), "contacts": str(contact_dir)}


def export_gbrain(conn, out_dir: Path, *, gbrain_bin: str | None = None, run_import: bool = False) -> dict[str, object]:
    paths = export_markdown(conn, out_dir)
    result: dict[str, object] = {"paths": paths, "imported": False}
    if run_import:
        bin_path = gbrain_bin or "gbrain"
        proc = subprocess.run([bin_path, "import", str(out_dir), "--no-embed"], text=True, capture_output=True)
        result.update({"imported": proc.returncode == 0, "stdout": proc.stdout, "stderr": proc.stderr, "returncode": proc.returncode})
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr or proc.stdout or "gbrain import failed")
    return result

