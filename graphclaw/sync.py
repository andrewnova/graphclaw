from __future__ import annotations

import json
from typing import Any, Callable

from .config import OrgConfig
from .db import finish_run, get_cursor, set_cursor, start_run, tx, upsert_raw
from .graph import GraphClient, path_quote, q


MAIL_SELECT = ",".join(
    [
        "id",
        "changeKey",
        "parentFolderId",
        "conversationId",
        "subject",
        "from",
        "receivedDateTime",
        "sentDateTime",
        "lastModifiedDateTime",
        "webLink",
        "bodyPreview",
        "importance",
        "isRead",
        "hasAttachments",
    ]
)

EVENT_SELECT = ",".join(
    [
        "id",
        "changeKey",
        "subject",
        "start",
        "end",
        "organizer",
        "attendees",
        "location",
        "webLink",
        "bodyPreview",
        "isAllDay",
        "isCancelled",
        "lastModifiedDateTime",
        "onlineMeetingUrl",
    ]
)

CONTACT_SELECT = ",".join(
    [
        "id",
        "changeKey",
        "displayName",
        "emailAddresses",
        "businessPhones",
        "mobilePhone",
        "companyName",
        "jobTitle",
        "lastModifiedDateTime",
    ]
)


def _email_address(value: dict[str, Any] | None) -> tuple[str | None, str | None]:
    email = (value or {}).get("emailAddress") or {}
    return email.get("name"), email.get("address")


def _run_delta(
    conn,
    cfg: OrgConfig,
    kind: str,
    account: str,
    scope: str,
    initial_url: str,
    handler: Callable[[dict[str, Any]], tuple[int, int]],
    max_pages: int | None = None,
) -> dict[str, Any]:
    client = GraphClient(cfg, conn)
    run_id = start_run(conn, kind, account, scope)
    fetched = upserted = deleted = 0
    url = get_cursor(conn, kind, account, scope) or initial_url
    pages = 0
    try:
        while url:
            pages += 1
            payload = client.get_json(url)
            items = payload.get("value") or []
            fetched += len(items)
            with tx(conn):
                for item in items:
                    u, d = handler(item)
                    upserted += u
                    deleted += d
            next_link = payload.get("@odata.nextLink")
            delta_link = payload.get("@odata.deltaLink")
            if delta_link:
                with tx(conn):
                    set_cursor(conn, kind, account, scope, delta_link)
                url = None
            else:
                url = next_link
            if max_pages and pages >= max_pages:
                break
        with tx(conn):
            finish_run(conn, run_id, "ok", fetched, upserted, deleted)
        return {"status": "ok", "kind": kind, "account": account, "scope": scope, "pages": pages, "fetched": fetched, "upserted": upserted, "deleted": deleted}
    except Exception as exc:
        with tx(conn):
            finish_run(conn, run_id, "error", fetched, upserted, deleted, str(exc))
        raise


def sync_mail(conn, cfg: OrgConfig, folder: str = "inbox", account: str | None = None, max_pages: int | None = None) -> dict[str, Any]:
    account = account or cfg.account
    folder_path = path_quote(folder)
    scope = f"folder:{folder}"
    initial = f"/me/mailFolders/{folder_path}/messages/delta?{q({'$select': MAIL_SELECT})}"

    def handle(item: dict[str, Any]) -> tuple[int, int]:
        rid = upsert_raw(conn, cfg, account, "microsoft-mail", "message", item)
        deleted = 1 if "@removed" in item else 0
        sender_name, sender_email = _email_address(item.get("from"))
        conn.execute(
            """
            INSERT INTO mail_messages
              (id, raw_item_id, org_id, account, folder_id, conversation_id, subject, sender_name, sender_email,
               received_at, sent_at, web_link, body_preview, importance, is_read, has_attachments, deleted)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              raw_item_id=excluded.raw_item_id,
              folder_id=excluded.folder_id,
              conversation_id=excluded.conversation_id,
              subject=excluded.subject,
              sender_name=excluded.sender_name,
              sender_email=excluded.sender_email,
              received_at=excluded.received_at,
              sent_at=excluded.sent_at,
              web_link=excluded.web_link,
              body_preview=excluded.body_preview,
              importance=excluded.importance,
              is_read=excluded.is_read,
              has_attachments=excluded.has_attachments,
              deleted=excluded.deleted
            """,
            (
                item.get("id"),
                rid,
                cfg.org,
                account,
                item.get("parentFolderId") or folder,
                item.get("conversationId"),
                item.get("subject"),
                sender_name,
                sender_email,
                item.get("receivedDateTime"),
                item.get("sentDateTime"),
                item.get("webLink"),
                item.get("bodyPreview"),
                item.get("importance"),
                int(bool(item.get("isRead"))) if "isRead" in item else None,
                int(bool(item.get("hasAttachments"))) if "hasAttachments" in item else None,
                deleted,
            ),
        )
        return (0 if deleted else 1, deleted)

    return _run_delta(conn, cfg, "mail", account, scope, initial, handle, max_pages=max_pages)


def sync_calendar(
    conn,
    cfg: OrgConfig,
    start: str,
    end: str,
    account: str | None = None,
    calendar_id: str | None = None,
    max_pages: int | None = None,
) -> dict[str, Any]:
    account = account or cfg.account
    scope = f"{calendar_id or 'default'}:{start}:{end}"
    prefix = f"/me/calendars/{path_quote(calendar_id)}/calendarView/delta" if calendar_id else "/me/calendarView/delta"
    initial = f"{prefix}?{q({'startDateTime': start, 'endDateTime': end, '$select': EVENT_SELECT})}"

    def handle(item: dict[str, Any]) -> tuple[int, int]:
        rid = upsert_raw(conn, cfg, account, "microsoft-calendar", "event", item)
        deleted = 1 if "@removed" in item else 0
        organizer_name, organizer_email = _email_address(item.get("organizer"))
        start_obj = item.get("start") or {}
        end_obj = item.get("end") or {}
        conn.execute(
            """
            INSERT INTO calendar_events
              (id, raw_item_id, org_id, account, calendar_scope, subject, start_at, end_at, time_zone,
               organizer_name, organizer_email, location, web_link, body_preview, is_all_day, is_cancelled, deleted)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              raw_item_id=excluded.raw_item_id,
              subject=excluded.subject,
              start_at=excluded.start_at,
              end_at=excluded.end_at,
              time_zone=excluded.time_zone,
              organizer_name=excluded.organizer_name,
              organizer_email=excluded.organizer_email,
              location=excluded.location,
              web_link=excluded.web_link,
              body_preview=excluded.body_preview,
              is_all_day=excluded.is_all_day,
              is_cancelled=excluded.is_cancelled,
              deleted=excluded.deleted
            """,
            (
                item.get("id"),
                rid,
                cfg.org,
                account,
                scope,
                item.get("subject"),
                start_obj.get("dateTime"),
                end_obj.get("dateTime"),
                start_obj.get("timeZone") or end_obj.get("timeZone"),
                organizer_name,
                organizer_email,
                (item.get("location") or {}).get("displayName"),
                item.get("webLink"),
                item.get("bodyPreview"),
                int(bool(item.get("isAllDay"))) if "isAllDay" in item else None,
                int(bool(item.get("isCancelled"))) if "isCancelled" in item else None,
                deleted,
            ),
        )
        return (0 if deleted else 1, deleted)

    return _run_delta(conn, cfg, "calendar", account, scope, initial, handle, max_pages=max_pages)


def sync_contacts(
    conn,
    cfg: OrgConfig,
    folder: str,
    account: str | None = None,
    max_pages: int | None = None,
) -> dict[str, Any]:
    account = account or cfg.account
    folder_path = path_quote(folder)
    scope = f"folder:{folder}"
    initial = f"/me/contactFolders/{folder_path}/contacts/delta?{q({'$select': CONTACT_SELECT})}"

    def handle(item: dict[str, Any]) -> tuple[int, int]:
        rid = upsert_raw(conn, cfg, account, "microsoft-contacts", "contact", item)
        deleted = 1 if "@removed" in item else 0
        phones = {
            "business": item.get("businessPhones") or [],
            "mobile": item.get("mobilePhone"),
        }
        conn.execute(
            """
            INSERT INTO contacts
              (id, raw_item_id, org_id, account, contact_folder_id, display_name, email_addresses, phones, company_name, job_title, deleted)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              raw_item_id=excluded.raw_item_id,
              display_name=excluded.display_name,
              email_addresses=excluded.email_addresses,
              phones=excluded.phones,
              company_name=excluded.company_name,
              job_title=excluded.job_title,
              deleted=excluded.deleted
            """,
            (
                item.get("id"),
                rid,
                cfg.org,
                account,
                folder,
                item.get("displayName"),
                json.dumps(item.get("emailAddresses") or [], ensure_ascii=False),
                json.dumps(phones, ensure_ascii=False),
                item.get("companyName"),
                item.get("jobTitle"),
                deleted,
            ),
        )
        return (0 if deleted else 1, deleted)

    return _run_delta(conn, cfg, "contacts", account, scope, initial, handle, max_pages=max_pages)

