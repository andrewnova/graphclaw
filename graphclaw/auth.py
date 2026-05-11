from __future__ import annotations

import json
import sqlite3
import time
import urllib.parse
import urllib.request
from typing import Any

from .config import OrgConfig
from .db import now_iso


class AuthError(RuntimeError):
    pass


def _post_form(url: str, data: dict[str, str]) -> dict[str, Any]:
    body = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/x-www-form-urlencoded"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        try:
            payload = json.loads(exc.read().decode("utf-8"))
        except Exception:
            payload = {"error": str(exc)}
        raise AuthError(payload.get("error_description") or payload.get("error") or str(exc)) from exc


def device_login(conn: sqlite3.Connection, cfg: OrgConfig) -> dict[str, Any]:
    device = _post_form(
        f"{cfg.authority}/oauth2/v2.0/devicecode",
        {"client_id": cfg.client_id, "scope": cfg.scope_string},
    )
    print(device.get("message") or f"Open {device['verification_uri']} and enter {device['user_code']}")
    interval = int(device.get("interval") or 5)
    expires_at = time.time() + int(device.get("expires_in") or 900)
    token_url = f"{cfg.authority}/oauth2/v2.0/token"
    while time.time() < expires_at:
        time.sleep(interval)
        try:
            token = _post_form(
                token_url,
                {
                    "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                    "client_id": cfg.client_id,
                    "device_code": device["device_code"],
                },
            )
            store_token(conn, cfg.account, cfg.scope_string, token)
            return token
        except AuthError as exc:
            msg = str(exc)
            if "authorization_pending" in msg:
                continue
            if "slow_down" in msg:
                interval += 5
                continue
            raise
    raise AuthError("device code expired before login completed")


def store_token(conn: sqlite3.Connection, account: str, scopes: str, token: dict[str, Any]) -> None:
    expires_at = int(time.time()) + int(token.get("expires_in") or 3600) - 60
    conn.execute(
        """
        INSERT INTO tokens (account, access_token, refresh_token, expires_at, scopes, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(account) DO UPDATE SET
          access_token=excluded.access_token,
          refresh_token=coalesce(excluded.refresh_token, tokens.refresh_token),
          expires_at=excluded.expires_at,
          scopes=excluded.scopes,
          updated_at=excluded.updated_at
        """,
        (account, token.get("access_token"), token.get("refresh_token"), expires_at, scopes, now_iso()),
    )
    conn.commit()


def token_status(conn: sqlite3.Connection, account: str) -> dict[str, Any]:
    row = conn.execute("SELECT account, expires_at, scopes, updated_at FROM tokens WHERE account=?", (account,)).fetchone()
    if not row:
        return {"account": account, "logged_in": False}
    return {
        "account": row["account"],
        "logged_in": True,
        "expires_at": int(row["expires_at"] or 0),
        "expires_in": int(row["expires_at"] or 0) - int(time.time()),
        "scopes": row["scopes"],
        "updated_at": row["updated_at"],
    }


def access_token(conn: sqlite3.Connection, cfg: OrgConfig) -> str:
    row = conn.execute("SELECT access_token, refresh_token, expires_at FROM tokens WHERE account=?", (cfg.account,)).fetchone()
    if not row:
        raise AuthError(f"not logged in for {cfg.org}/{cfg.account}. Run `graphclaw auth login --org {cfg.org}`.")
    if int(row["expires_at"] or 0) > int(time.time()) + 120:
        return str(row["access_token"])
    refresh = row["refresh_token"]
    if not refresh:
        raise AuthError("access token expired and no refresh token is stored")
    token = _post_form(
        f"{cfg.authority}/oauth2/v2.0/token",
        {
            "client_id": cfg.client_id,
            "grant_type": "refresh_token",
            "refresh_token": str(refresh),
            "scope": cfg.scope_string,
        },
    )
    store_token(conn, cfg.account, cfg.scope_string, token)
    return str(token["access_token"])

