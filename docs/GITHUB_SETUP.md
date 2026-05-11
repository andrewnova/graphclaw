# GitHub Setup

Recommended repository settings for `andrewnova/graphclaw`.

## About

Description:

```text
Local-first Microsoft 365 source collector for agents: Outlook mail, calendar, contacts, SQLite, Graph delta APIs, and deterministic exports.
```

Website:

```text
https://andrewnova.github.io/graphclaw/
```

Topics:

```text
microsoft-graph outlook calendar contacts sqlite agents local-first cli python
```

## Pages

Repository Settings -> Pages:

- Source: GitHub Actions
- The workflow in `.github/workflows/pages.yml` publishes `site/`

## Social Preview

Repository Settings -> Social preview:

- Upload `assets/social-card.svg`
- If GitHub requires PNG, export it to 1280x640 PNG first.

## Optional Custom Domain

If you buy `graphclaw.sh`:

```bash
echo graphclaw.sh > site/CNAME
```

Then configure DNS for GitHub Pages.
