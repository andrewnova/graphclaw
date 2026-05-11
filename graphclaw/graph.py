from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from .auth import access_token
from .config import OrgConfig


class GraphError(RuntimeError):
    pass


class GraphClient:
    def __init__(self, cfg: OrgConfig, conn):
        self.cfg = cfg
        self.conn = conn

    def get_json(self, url: str, *, prefer: str | None = "odata.maxpagesize=50") -> dict[str, Any]:
        if url.startswith("https://"):
            full_url = url
        else:
            full_url = self.cfg.graph_base.rstrip("/") + "/" + url.lstrip("/")
        headers = {
            "Authorization": f"Bearer {access_token(self.conn, self.cfg)}",
            "Accept": "application/json",
        }
        if prefer:
            headers["Prefer"] = prefer
        for attempt in range(5):
            req = urllib.request.Request(full_url, headers=headers)
            try:
                with urllib.request.urlopen(req, timeout=60) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                if exc.code in (429, 503, 504):
                    retry = exc.headers.get("Retry-After")
                    delay = int(retry) if retry and retry.isdigit() else min(60, 2 ** attempt)
                    time.sleep(delay)
                    continue
                try:
                    payload = json.loads(exc.read().decode("utf-8"))
                except Exception:
                    payload = {"error": {"message": str(exc)}}
                message = payload.get("error", {}).get("message") or payload.get("error") or str(exc)
                raise GraphError(f"Graph HTTP {exc.code}: {message}") from exc
        raise GraphError(f"Graph request failed after retries: {full_url}")


def q(params: dict[str, str]) -> str:
    return urllib.parse.urlencode(params, safe=",$")


def path_quote(value: str) -> str:
    return urllib.parse.quote(value, safe="")

