from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .config import OrgConfig, org_db_path


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def connect(cfg: OrgConfig) -> sqlite3.Connection:
    path = org_db_path(cfg.org)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    init_db(conn, cfg)
    os.chmod(path, 0o600)
    return conn


@contextmanager
def tx(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def init_db(conn: sqlite3.Connection, cfg: OrgConfig) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS orgs (
          id TEXT PRIMARY KEY,
          tenant TEXT NOT NULL,
          client_id TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS accounts (
          account TEXT PRIMARY KEY,
          org_id TEXT NOT NULL,
          display_name TEXT,
          user_principal_name TEXT,
          raw_json TEXT,
          updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS tokens (
          account TEXT PRIMARY KEY,
          access_token TEXT,
          refresh_token TEXT,
          expires_at INTEGER,
          scopes TEXT,
          updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS sync_cursors (
          kind TEXT NOT NULL,
          account TEXT NOT NULL,
          scope TEXT NOT NULL,
          delta_link TEXT,
          updated_at TEXT NOT NULL,
          PRIMARY KEY (kind, account, scope)
        );
        CREATE TABLE IF NOT EXISTS sync_runs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          kind TEXT NOT NULL,
          account TEXT NOT NULL,
          scope TEXT NOT NULL,
          started_at TEXT NOT NULL,
          finished_at TEXT,
          status TEXT NOT NULL,
          fetched INTEGER NOT NULL DEFAULT 0,
          upserted INTEGER NOT NULL DEFAULT 0,
          deleted INTEGER NOT NULL DEFAULT 0,
          error TEXT
        );
        CREATE TABLE IF NOT EXISTS raw_items (
          id TEXT PRIMARY KEY,
          org_id TEXT NOT NULL,
          account TEXT NOT NULL,
          source TEXT NOT NULL,
          external_id TEXT NOT NULL,
          change_key TEXT,
          item_type TEXT NOT NULL,
          deleted INTEGER NOT NULL DEFAULT 0,
          raw_json TEXT NOT NULL,
          received_at TEXT,
          updated_at TEXT,
          ingested_at TEXT NOT NULL,
          UNIQUE (org_id, account, source, external_id)
        );
        CREATE TABLE IF NOT EXISTS mail_messages (
          id TEXT PRIMARY KEY,
          raw_item_id TEXT NOT NULL REFERENCES raw_items(id) ON DELETE CASCADE,
          org_id TEXT NOT NULL,
          account TEXT NOT NULL,
          folder_id TEXT,
          conversation_id TEXT,
          subject TEXT,
          sender_name TEXT,
          sender_email TEXT,
          received_at TEXT,
          sent_at TEXT,
          web_link TEXT,
          body_preview TEXT,
          importance TEXT,
          is_read INTEGER,
          has_attachments INTEGER,
          deleted INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_mail_received ON mail_messages(received_at);
        CREATE INDEX IF NOT EXISTS idx_mail_sender ON mail_messages(sender_email);
        CREATE TABLE IF NOT EXISTS calendar_events (
          id TEXT PRIMARY KEY,
          raw_item_id TEXT NOT NULL REFERENCES raw_items(id) ON DELETE CASCADE,
          org_id TEXT NOT NULL,
          account TEXT NOT NULL,
          calendar_scope TEXT,
          subject TEXT,
          start_at TEXT,
          end_at TEXT,
          time_zone TEXT,
          organizer_name TEXT,
          organizer_email TEXT,
          location TEXT,
          web_link TEXT,
          body_preview TEXT,
          is_all_day INTEGER,
          is_cancelled INTEGER,
          deleted INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_events_start ON calendar_events(start_at);
        CREATE TABLE IF NOT EXISTS contacts (
          id TEXT PRIMARY KEY,
          raw_item_id TEXT NOT NULL REFERENCES raw_items(id) ON DELETE CASCADE,
          org_id TEXT NOT NULL,
          account TEXT NOT NULL,
          contact_folder_id TEXT,
          display_name TEXT,
          email_addresses TEXT,
          phones TEXT,
          company_name TEXT,
          job_title TEXT,
          deleted INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_contacts_name ON contacts(display_name);
        """
    )
    ts = now_iso()
    conn.execute(
        """
        INSERT INTO orgs (id, tenant, client_id, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET tenant=excluded.tenant, client_id=excluded.client_id, updated_at=excluded.updated_at
        """,
        (cfg.org, cfg.tenant, cfg.client_id, ts, ts),
    )
    conn.commit()


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def item_deleted(item: dict[str, Any]) -> bool:
    return "@removed" in item


def raw_item_id(org: str, account: str, source: str, external_id: str) -> str:
    return f"{org}:{account}:{source}:{external_id}"


def upsert_raw(
    conn: sqlite3.Connection,
    cfg: OrgConfig,
    account: str,
    source: str,
    item_type: str,
    item: dict[str, Any],
) -> str:
    external_id = str(item.get("id") or item.get("@odata.id") or "")
    if not external_id:
        raise ValueError("Graph item missing id")
    deleted = 1 if item_deleted(item) else 0
    change_key = item.get("changeKey") or item.get("lastModifiedDateTime")
    received_at = item.get("receivedDateTime") or item.get("createdDateTime") or item.get("start", {}).get("dateTime")
    updated_at = item.get("lastModifiedDateTime")
    rid = raw_item_id(cfg.org, account, source, external_id)
    conn.execute(
        """
        INSERT INTO raw_items
          (id, org_id, account, source, external_id, change_key, item_type, deleted, raw_json, received_at, updated_at, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
          change_key=excluded.change_key,
          deleted=excluded.deleted,
          raw_json=excluded.raw_json,
          received_at=excluded.received_at,
          updated_at=excluded.updated_at,
          ingested_at=excluded.ingested_at
        """,
        (rid, cfg.org, account, source, external_id, change_key, item_type, deleted, json_dumps(item), received_at, updated_at, now_iso()),
    )
    return rid


def set_cursor(conn: sqlite3.Connection, kind: str, account: str, scope: str, delta_link: str) -> None:
    conn.execute(
        """
        INSERT INTO sync_cursors (kind, account, scope, delta_link, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(kind, account, scope) DO UPDATE SET
          delta_link=excluded.delta_link,
          updated_at=excluded.updated_at
        """,
        (kind, account, scope, delta_link, now_iso()),
    )


def get_cursor(conn: sqlite3.Connection, kind: str, account: str, scope: str) -> str | None:
    row = conn.execute(
        "SELECT delta_link FROM sync_cursors WHERE kind=? AND account=? AND scope=?",
        (kind, account, scope),
    ).fetchone()
    return str(row["delta_link"]) if row and row["delta_link"] else None


def start_run(conn: sqlite3.Connection, kind: str, account: str, scope: str) -> int:
    cur = conn.execute(
        "INSERT INTO sync_runs (kind, account, scope, started_at, status) VALUES (?, ?, ?, ?, ?)",
        (kind, account, scope, now_iso(), "running"),
    )
    return int(cur.lastrowid)


def finish_run(
    conn: sqlite3.Connection,
    run_id: int,
    status: str,
    fetched: int,
    upserted: int,
    deleted: int,
    error: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE sync_runs
        SET finished_at=?, status=?, fetched=?, upserted=?, deleted=?, error=?
        WHERE id=?
        """,
        (now_iso(), status, fetched, upserted, deleted, error, run_id),
    )


def stats(conn: sqlite3.Connection) -> dict[str, Any]:
    def count(table: str) -> int:
        return int(conn.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"])

    latest = conn.execute(
        "SELECT kind, account, scope, status, finished_at, fetched, upserted, deleted, error FROM sync_runs ORDER BY id DESC LIMIT 10"
    ).fetchall()
    return {
        "mail_messages": count("mail_messages"),
        "calendar_events": count("calendar_events"),
        "contacts": count("contacts"),
        "raw_items": count("raw_items"),
        "sync_cursors": count("sync_cursors"),
        "recent_runs": [dict(row) for row in latest],
    }

