from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


GRAPH_BASE = "https://graph.microsoft.com/v1.0"
TOKEN_BASE = "https://login.microsoftonline.com"
DEFAULT_SCOPES = [
    "offline_access",
    "User.Read",
    "Mail.Read",
    "Calendars.Read",
    "Contacts.Read",
]


class ConfigError(RuntimeError):
    pass


def home_dir() -> Path:
    return Path(os.environ.get("GRAPHCLAW_HOME", Path.home() / ".graphclaw")).expanduser()


def validate_slug(slug: str) -> str:
    value = slug.strip().lower()
    if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,62}", value):
        raise ConfigError("org slug must be 1-63 chars: lowercase letters, numbers, dot, underscore, dash")
    return value


def org_dir(org: str) -> Path:
    return home_dir() / "orgs" / validate_slug(org)


def org_config_path(org: str) -> Path:
    return org_dir(org) / "org.json"


def org_db_path(org: str) -> Path:
    return org_dir(org) / "graphclaw.sqlite"


@dataclass(frozen=True)
class OrgConfig:
    org: str
    tenant: str
    client_id: str
    account: str
    scopes: list[str]
    graph_base: str = GRAPH_BASE
    token_base: str = TOKEN_BASE

    @property
    def authority(self) -> str:
        return f"{self.token_base.rstrip('/')}/{self.tenant}"

    @property
    def scope_string(self) -> str:
        return " ".join(self.scopes)

    def to_json(self) -> dict[str, Any]:
        return {
            "org": self.org,
            "tenant": self.tenant,
            "client_id": self.client_id,
            "account": self.account,
            "scopes": self.scopes,
            "graph_base": self.graph_base,
            "token_base": self.token_base,
        }


def save_org_config(cfg: OrgConfig) -> None:
    path = org_config_path(cfg.org)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cfg.to_json(), indent=2) + "\n", encoding="utf-8")
    os.chmod(path, 0o600)


def load_org_config(org: str) -> OrgConfig:
    path = org_config_path(org)
    if not path.exists():
        raise ConfigError(f"org not configured: {org}. Run `graphclaw org add {org} ...` first.")
    data = json.loads(path.read_text(encoding="utf-8"))
    return OrgConfig(
        org=validate_slug(data["org"]),
        tenant=data.get("tenant") or "common",
        client_id=data["client_id"],
        account=data.get("account") or "me",
        scopes=list(data.get("scopes") or DEFAULT_SCOPES),
        graph_base=data.get("graph_base") or GRAPH_BASE,
        token_base=data.get("token_base") or TOKEN_BASE,
    )


def list_orgs() -> list[OrgConfig]:
    root = home_dir() / "orgs"
    if not root.exists():
        return []
    orgs: list[OrgConfig] = []
    for child in sorted(root.iterdir()):
        if child.is_dir() and (child / "org.json").exists():
            orgs.append(load_org_config(child.name))
    return orgs

