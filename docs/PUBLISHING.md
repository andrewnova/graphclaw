# Publishing

Graphclaw ships with a static landing page at `site/index.html`.

## GitHub Pages

This repo includes `.github/workflows/pages.yml`, which publishes the `site/`
folder to GitHub Pages on every push to `main`.

1. Push this repo to GitHub.
2. In repository settings, enable Pages.
3. Set the source to **GitHub Actions**.
4. The public URL should be `https://andrewnova.github.io/graphclaw/`.

## Custom Domain

For a domain like `graphclaw.sh`, point DNS at GitHub Pages and add a `CNAME`
file in `site/`.

```bash
echo graphclaw.sh > site/CNAME
```

## Release

Recommended first release checklist:

```bash
python3 -m unittest discover -s tests -v
python3 -m graphclaw doctor
git tag v0.1.0
git push origin main --tags
```
