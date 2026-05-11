from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from . import __version__
from .auth import AuthError, device_login, token_status
from .config import DEFAULT_SCOPES, ConfigError, OrgConfig, home_dir, list_orgs, load_org_config, save_org_config, validate_slug
from .db import connect, stats
from .exporter import export_gbrain, export_jsonl, export_markdown
from .graph import GraphClient, GraphError
from .sync import sync_calendar, sync_contacts, sync_mail


def emit(value, as_json: bool) -> None:
    if as_json:
        print(json.dumps(value, indent=2, ensure_ascii=False))
    elif isinstance(value, str):
        print(value)
    else:
        print(json.dumps(value, indent=2, ensure_ascii=False))


def add_global(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--json", action="store_true", help="emit JSON")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="graphclaw", description="Local-first Microsoft Graph mirror for GBrain")
    parser.add_argument("--version", action="version", version=__version__)
    add_global(parser)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("doctor", help="check local setup")

    org = sub.add_parser("org", help="manage org/company configs")
    org_sub = org.add_subparsers(dest="org_command", required=True)
    org_add = org_sub.add_parser("add", help="add or update an org")
    org_add.add_argument("org")
    org_add.add_argument("--client-id", required=True)
    org_add.add_argument("--tenant", default="common")
    org_add.add_argument("--account", default="me")
    org_add.add_argument("--scope", action="append", dest="scopes", help="OAuth scope; repeatable")
    org_sub.add_parser("list", help="list orgs")
    org_show = org_sub.add_parser("show", help="show org config")
    org_show.add_argument("org")

    auth = sub.add_parser("auth", help="authenticate with Microsoft Graph")
    auth_sub = auth.add_subparsers(dest="auth_command", required=True)
    auth_login = auth_sub.add_parser("login", help="device-code login")
    auth_login.add_argument("--org", required=True)
    auth_status = auth_sub.add_parser("status", help="show token status")
    auth_status.add_argument("--org", required=True)

    sync = sub.add_parser("sync", help="sync Microsoft Graph sources")
    sync_sub = sync.add_subparsers(dest="sync_command", required=True)
    mail = sync_sub.add_parser("mail", help="sync mail folder delta")
    mail.add_argument("--org", required=True)
    mail.add_argument("--folder", default="inbox", help="folder id or well-known folder, default inbox")
    mail.add_argument("--max-pages", type=int)
    cal = sync_sub.add_parser("calendar", help="sync calendarView delta")
    cal.add_argument("--org", required=True)
    cal.add_argument("--start", required=True, help="inclusive ISO date/datetime")
    cal.add_argument("--end", required=True, help="exclusive ISO date/datetime")
    cal.add_argument("--calendar-id")
    cal.add_argument("--max-pages", type=int)
    contacts = sync_sub.add_parser("contacts", help="sync contacts delta for a contact folder")
    contacts.add_argument("--org", required=True)
    contacts.add_argument("--folder", required=True, help="contact folder id")
    contacts.add_argument("--max-pages", type=int)

    list_cmd = sub.add_parser("list", help="list Graph containers")
    list_sub = list_cmd.add_subparsers(dest="list_command", required=True)
    for name in ("mail-folders", "calendars", "contact-folders"):
        item = list_sub.add_parser(name)
        item.add_argument("--org", required=True)

    db = sub.add_parser("db", help="database utilities")
    db_sub = db.add_subparsers(dest="db_command", required=True)
    db_stats = db_sub.add_parser("stats", help="show local database stats")
    db_stats.add_argument("--org", required=True)

    exp = sub.add_parser("export", help="export local mirror")
    exp_sub = exp.add_subparsers(dest="export_command", required=True)
    for name in ("jsonl", "markdown", "gbrain"):
        e = exp_sub.add_parser(name)
        e.add_argument("--org", required=True)
        e.add_argument("--out", required=True)
        if name == "gbrain":
            e.add_argument("--run-import", action="store_true")
            e.add_argument("--gbrain-bin")
    return parser


def normalize_argv(argv: list[str] | None) -> list[str]:
    raw = list(sys.argv[1:] if argv is None else argv)
    if "--json" in raw:
        raw = [part for part in raw if part != "--json"]
        raw.insert(0, "--json")
    return raw


def org_config_from_args(args) -> OrgConfig:
    scopes = args.scopes if args.scopes else DEFAULT_SCOPES
    return OrgConfig(
        org=validate_slug(args.org),
        tenant=args.tenant,
        client_id=args.client_id,
        account=args.account,
        scopes=scopes,
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(normalize_argv(argv))
    try:
        if args.command == "doctor":
            root = home_dir()
            orgs = list_orgs()
            emit(
                {
                    "version": __version__,
                    "home": str(root),
                    "home_exists": root.exists(),
                    "orgs": [o.org for o in orgs],
                    "status": "ok",
                },
                args.json,
            )
            return 0
        if args.command == "org":
            if args.org_command == "add":
                cfg = org_config_from_args(args)
                save_org_config(cfg)
                with connect(cfg):
                    pass
                emit({"status": "ok", "org": cfg.org, "path": str(home_dir() / "orgs" / cfg.org)}, args.json)
                return 0
            if args.org_command == "list":
                emit({"orgs": [o.to_json() for o in list_orgs()]}, args.json)
                return 0
            if args.org_command == "show":
                cfg = load_org_config(args.org)
                data = cfg.to_json()
                data["client_id"] = data["client_id"][:8] + "..." if data["client_id"] else ""
                emit(data, args.json)
                return 0
        if args.command == "auth":
            cfg = load_org_config(args.org)
            conn = connect(cfg)
            if args.auth_command == "login":
                token = device_login(conn, cfg)
                emit({"status": "ok", "org": cfg.org, "account": cfg.account, "expires_in": token.get("expires_in")}, args.json)
                return 0
            if args.auth_command == "status":
                emit(token_status(conn, cfg.account), args.json)
                return 0
        if args.command == "sync":
            cfg = load_org_config(args.org)
            conn = connect(cfg)
            if args.sync_command == "mail":
                emit(sync_mail(conn, cfg, folder=args.folder, max_pages=args.max_pages), args.json)
                return 0
            if args.sync_command == "calendar":
                emit(sync_calendar(conn, cfg, start=args.start, end=args.end, calendar_id=args.calendar_id, max_pages=args.max_pages), args.json)
                return 0
            if args.sync_command == "contacts":
                emit(sync_contacts(conn, cfg, folder=args.folder, max_pages=args.max_pages), args.json)
                return 0
        if args.command == "list":
            cfg = load_org_config(args.org)
            conn = connect(cfg)
            client = GraphClient(cfg, conn)
            if args.list_command == "mail-folders":
                emit({"mail_folders": collect_all(client, "/me/mailFolders?$select=id,displayName,parentFolderId,totalItemCount,unreadItemCount")}, args.json)
                return 0
            if args.list_command == "calendars":
                emit({"calendars": collect_all(client, "/me/calendars?$select=id,name,canEdit,owner")}, args.json)
                return 0
            if args.list_command == "contact-folders":
                emit({"contact_folders": collect_all(client, "/me/contactFolders?$select=id,displayName,parentFolderId")}, args.json)
                return 0
        if args.command == "db":
            cfg = load_org_config(args.org)
            conn = connect(cfg)
            emit(stats(conn), args.json)
            return 0
        if args.command == "export":
            cfg = load_org_config(args.org)
            conn = connect(cfg)
            out = Path(args.out).expanduser()
            if args.export_command == "jsonl":
                emit(export_jsonl(conn, out), args.json)
                return 0
            if args.export_command == "markdown":
                emit(export_markdown(conn, out), args.json)
                return 0
            if args.export_command == "gbrain":
                emit(export_gbrain(conn, out, gbrain_bin=args.gbrain_bin, run_import=args.run_import), args.json)
                return 0
    except (ConfigError, AuthError, GraphError, RuntimeError, ValueError) as exc:
        if args.json:
            print(json.dumps({"status": "error", "error": str(exc)}, indent=2), file=sys.stderr)
        else:
            print(f"error: {exc}", file=sys.stderr)
        return 1
    parser.error("unhandled command")
    return 2


def collect_all(client: GraphClient, url: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    next_url: str | None = url
    while next_url:
        payload = client.get_json(next_url)
        rows.extend(payload.get("value") or [])
        next_url = payload.get("@odata.nextLink")
    return rows
