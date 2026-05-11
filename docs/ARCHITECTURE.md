# Architecture

Graphclaw follows the deterministic collector pattern:

```text
Microsoft Graph
  -> Graphclaw SQLite mirror
  -> JSONL / Markdown exports
  -> agent source import
  -> LLM enrichment and entity pages
```

The collector does mechanical work:

- auth refresh
- pagination
- delta cursor storage
- tombstone handling
- raw JSON preservation
- deterministic Outlook links

The agent does judgment work:

- priority classification
- entity detection
- relationship interpretation
- people/company page updates

## Why SQLite Per Org

Raw source mirrors should be boring and isolated. A client or company export
should be movable as one file, debuggable with `sqlite3`, and removable without
affecting another org.

Your agent memory store is the intelligence layer. Graphclaw is the provenance
layer.

## Schema

- `raw_items`: full Graph payloads, one row per external item
- `mail_messages`: query-friendly mail projection
- `calendar_events`: query-friendly event projection
- `contacts`: query-friendly contact projection
- `sync_cursors`: saved `@odata.deltaLink` by scope
- `sync_runs`: audit log
- `tokens`: delegated OAuth token cache

## Scope Strategy

Use least privilege:

- `User.Read`
- `Mail.Read`
- `Calendars.Read`
- `Contacts.Read`
- `offline_access`

Write scopes belong in a separate explicit action tool, not the ingestion
collector.
