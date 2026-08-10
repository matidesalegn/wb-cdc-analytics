#!/usr/bin/env bash
# Render diagrams/src/*.mmd to SVG and PNG.
#
# OPTIONAL. The Mermaid source is embedded directly in the README and the design report as
# fenced ```mermaid blocks, which GitHub renders natively, so nobody needs this to read
# the diagrams. It exists for the PDF export, where a rendered image is required.
#
# First run downloads a Chromium via npx, which is slow and needs network.
set -euo pipefail
cd "$(dirname "$0")/.."

command -v npx > /dev/null || { echo "npx not found; install Node, or just read the mermaid blocks in the README" >&2; exit 1; }
mkdir -p diagrams/exports

cat > /tmp/puppeteer-config.json <<'JSON'
{"args": ["--no-sandbox", "--disable-setuid-sandbox"]}
JSON

for src in diagrams/src/*.mmd; do
  name=$(basename "$src" .mmd)
  for fmt in svg png; do
    echo "rendering ${name}.${fmt}"
    npx -y @mermaid-js/mermaid-cli -i "$src" -o "diagrams/exports/${name}.${fmt}" \
      -p /tmp/puppeteer-config.json -b transparent -w 1800 > /dev/null
  done
done
echo "rendered into diagrams/exports/"
