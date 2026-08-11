#!/usr/bin/env python3
"""Render docs/design-report.md to docs/design-report.pdf.

    python3 scripts/render_report_pdf.py

WHY THIS EXISTS RATHER THAN A MANUAL EXPORT. The PDF is the copy that gets emailed, so it is
the copy most likely to drift from the Markdown that gets reviewed. Generating it from the
same source with one command means the two cannot disagree, and it makes the artifact
reproducible by anyone who has the repository.

WHY CHROMIUM AND NOT WEASYPRINT. weasyprint is broken on this machine and debugging it is not
on the critical path. Chromium is already present because mermaid-cli vendors one to render
the diagrams, so this adds no dependency that the repository did not already need.

DESIGN NOTES
  Images are inlined as base64 data URIs rather than referenced by path. A file:// page
  loading sibling files is subject to Chromium's local-file restrictions, and the failure mode
  is a silently missing diagram in an otherwise fine-looking PDF, which is exactly the sort of
  defect that survives to a reviewer.

  The default Chromium print header and footer are suppressed. They render the source file's
  full path across the top of every page, which looks like a draft someone printed by mistake.
"""

from __future__ import annotations

import base64
import mimetypes
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile

REPO = pathlib.Path(__file__).resolve().parent.parent
SRC = REPO / "docs" / "design-report.md"
OUT = REPO / "docs" / "design-report.pdf"

# A4 portrait, 16mm side margins, so the content column is 178mm. Both rendered diagrams are
# taller than they are wide, and at 178mm wide they each come in under the 261mm of vertical
# content space, which is why `width: 100%` is safe rather than a gamble.
CSS = """
@page { size: A4 portrait; margin: 18mm 16mm 20mm 16mm; }
/* Named pages, so each diagram gets a sheet of its own with tighter margins: 194mm of column
   instead of 178mm. Deliberately PORTRAIT. Landscape looks like the obvious choice for a
   diagram and is wrong for these two, because both are taller than they are wide, so the
   binding constraint is height and a landscape sheet has less of it. Tried it: each figure
   overflowed onto three pages. */
@page figure { size: A4 portrait; margin: 8mm; }
.figure-wide { page: figure; break-before: page; break-after: page; }
.figure-wide svg { width: 100%; height: auto; max-height: 272mm; display: block; }
.figure-cap { font-size: 8.5pt; color: #55554f; margin: 1.5mm 0 0; text-align: center; }
html { -webkit-print-color-adjust: exact; print-color-adjust: exact; }
body {
  font: 10.5pt/1.55 "Charter", "Bitstream Charter", "Georgia", "DejaVu Serif", serif;
  color: #16161a; margin: 0;
}
h1 { font-size: 20pt; margin: 0 0 2mm; letter-spacing: -0.01em; line-height: 1.2; }
h2 {
  font-size: 13pt; margin: 9mm 0 2.5mm; padding-bottom: 1.2mm;
  border-bottom: 0.6pt solid #c8c8c2; break-after: avoid; page-break-after: avoid;
}
h3 { font-size: 11pt; margin: 6mm 0 1.8mm; break-after: avoid; page-break-after: avoid; }
h2 + p, h3 + p { margin-top: 0; }
p { margin: 0 0 2.6mm; orphans: 3; widows: 3; text-align: justify; hyphens: auto; }
strong { font-weight: 700; }
em { font-style: italic; }
code {
  font: 9pt/1.4 "DejaVu Sans Mono", "Menlo", monospace;
  background: #f2f2ee; padding: 0.3mm 0.9mm; border-radius: 1pt;
  /* Long identifiers must break rather than push the column wider than the page, but only
     when there is no alternative: `anywhere` splits raw.cdc_event_log into "raw.cdc_event_lo"
     and "g", which reads as a rendering fault rather than a wrap. */
  overflow-wrap: break-word;
}
/* A table's first column is almost always an identifier, so let it claim the width it needs
   and give the prose column the remainder, rather than hyphenating table names. */
td:first-child code, th:first-child { white-space: nowrap; }
table code { font-size: 8.2pt; }
a { color: #16161a; text-decoration: none; border-bottom: 0.4pt dotted #8a8a84; }
ul, ol { margin: 0 0 2.6mm; padding-left: 5.5mm; }
li { margin: 0 0 1.1mm; }
table {
  width: 100%; border-collapse: collapse; margin: 2.5mm 0 4mm;
  font-size: 8.8pt; break-inside: auto;
}
th, td {
  border: 0.5pt solid #cfcfc9; padding: 1.3mm 1.8mm;
  text-align: left; vertical-align: top;
}
th { background: #f2f2ee; font-weight: 700; }
tr { break-inside: avoid; page-break-inside: avoid; }
img {
  width: 100%; height: auto; display: block; margin: 3mm 0 2mm;
  break-inside: avoid; page-break-inside: avoid;
}
hr { border: 0; border-top: 0.5pt solid #cfcfc9; margin: 6mm 0; }
blockquote {
  margin: 2.5mm 0; padding: 0 0 0 3.5mm; border-left: 1.2pt solid #b8562f; color: #3a3a36;
}
.titleblock { border-bottom: 1.2pt solid #16161a; padding-bottom: 3mm; margin-bottom: 6mm; }
.byline { font-size: 9.5pt; color: #55554f; margin: 0; }
.footnote {
  margin-top: 8mm; padding-top: 2.5mm; border-top: 0.5pt solid #cfcfc9;
  font-size: 8.5pt; color: #55554f;
}
"""


def find_chromium() -> str:
    """Prefer the Chromium mermaid-cli already vendored; fall back to a system one.

    Snap-packaged Chromium is deliberately last: snap confinement frequently cannot read a
    file:// page outside the user's home, and it fails in a way that looks like a rendering
    bug rather than a permissions one.
    """
    vendored = sorted(
        (pathlib.Path.home() / ".cache" / "puppeteer" / "chrome").glob("*/chrome-linux64/chrome")
    )
    for candidate in reversed(vendored):
        if candidate.is_file():
            return str(candidate)
    for name in ("google-chrome", "chromium-browser", "chromium"):
        found = shutil.which(name)
        if found:
            return found
    sys.exit("no Chromium found. Install one, or run `make render` once so mermaid-cli fetches it.")


def inline_images(html: str, base: pathlib.Path) -> str:
    """Replace each <img> with inline SVG markup where available, else a base64 data URI.

    Operates on the WHOLE tag. Matching only as far as src="..." and substituting there leaves
    the tag's remaining attributes stranded in the document as literal text, which shows up as
    a stray '>' next to the figure and, worse, stops the figure wrapper from matching so the
    sizing CSS never applies and the diagram renders at its default size.

    Inline SVG rather than a raster: Chromium keeps it as vector in the PDF, so the labels stay
    sharp at any zoom and the diagram text lands in the PDF's text layer, which means a reader
    can search for a table name and find it inside the figure.
    """
    missing: list[str] = []

    def repl(match: re.Match[str]) -> str:
        tag = match.group(0)
        src_match = re.search(r'\bsrc="([^"]+)"', tag)
        if not src_match:
            return tag
        src = src_match.group(1)
        if src.startswith(("data:", "http://", "https://")):
            return tag
        path = (base / src).resolve()
        if not path.is_file():
            missing.append(src)
            return tag
        svg = path.with_suffix(".svg")
        if svg.is_file():
            markup = svg.read_text(encoding="utf-8")
            markup = markup[markup.index("<svg") :]
            # mermaid pins a pixel width/height and a max-width style; all three have to go or
            # the figure ignores the column width it is given.
            markup = re.sub(r'\s(?:width|height)="[^"]*"', "", markup, count=2)
            markup = re.sub(r'\smax-width:\s*[^;"]*;?', "", markup)
            alt_match = re.search(r'\balt="([^"]*)"', tag)
            alt = alt_match.group(1) if alt_match else ""
            return f'<span data-alt="{alt}">{markup}</span>'
        mime = mimetypes.guess_type(path.name)[0] or "image/png"
        b64 = base64.b64encode(path.read_bytes()).decode("ascii")
        return re.sub(r'\bsrc="[^"]+"', f'src="data:{mime};base64,{b64}"', tag)

    html = re.sub(r"<img\b[^>]*?/?>", repl, html)
    if missing:
        # Loud, because a missing diagram is invisible in the output but fatal to the
        # deliverable: the report's own text refers to figures the reader cannot see.
        sys.exit(f"image(s) not found, refusing to emit a PDF with gaps: {missing}")
    return html


def promote_figures(html: str) -> str:
    """Give each standalone image its own landscape page, with a caption.

    Matches only a paragraph that contains nothing but an image, so an inline icon in running
    text would be left alone.
    """
    pattern = re.compile(r'<p>(?:<span data-alt="([^"]*)">(.*?)</span>|(<img\b[^>]*>))</p>', re.S)

    def repl(match: re.Match[str]) -> str:
        alt = match.group(1) or ""
        figure = match.group(2) or match.group(3) or ""
        source = {
            "Architecture": "diagrams/src/architecture.mmd",
            "ERD": "diagrams/src/erd.mmd",
        }.get(alt)
        cap = f"{alt}. Full resolution: diagrams/exports/. Source: {source}" if source else alt
        return f'<div class="figure-wide">{figure}<p class="figure-cap">{cap}</p></div>'

    return pattern.sub(repl, html)


def main() -> int:
    try:
        import markdown
    except ImportError:
        sys.exit("python-markdown missing: pip install markdown")

    if not SRC.is_file():
        sys.exit(f"missing {SRC}")
    text = SRC.read_text(encoding="utf-8")

    for marker in ("[YOUR VOICE]", "TODO", "FIXME"):
        if marker in text:
            sys.exit(f"refusing to build: {SRC.name} still contains {marker!r}")

    body = markdown.markdown(
        text,
        extensions=["tables", "fenced_code", "attr_list", "sane_lists"],
        output_format="html5",
    )
    body = inline_images(body, SRC.parent)
    body = promote_figures(body)

    # The report's own H1 becomes the title block, so the PDF opens like a document rather
    # than a web page that happened to be printed.
    body = body.replace(
        "<h1>Design report</h1>",
        '<div class="titleblock"><h1>Design report</h1>'
        '<p class="byline">wb-cdc-analytics &middot; end-to-end analytics engineering '
        "pipeline</p></div>",
        1,
    )

    html = (
        "<!doctype html><html lang='en'><head><meta charset='utf-8'>"
        "<title>Design report - wb-cdc-analytics</title>"
        f"<style>{CSS}</style></head><body>{body}</body></html>"
    )

    with tempfile.TemporaryDirectory() as tmp:
        page = pathlib.Path(tmp) / "report.html"
        page.write_text(html, encoding="utf-8")
        chromium = find_chromium()
        cmd = [
            chromium,
            "--headless",
            "--disable-gpu",
            "--no-sandbox",
            "--no-pdf-header-footer",
            "--run-all-compositor-stages-before-draw",
            "--virtual-time-budget=10000",
            f"--print-to-pdf={OUT}",
            page.as_uri(),
        ]
        # S603 is suppressed deliberately: every element of cmd is a literal or a path this
        # script resolved itself, there is no shell, and nothing comes from user input.
        result = subprocess.run(  # noqa: S603
            cmd, capture_output=True, text=True, timeout=180
        )
        if not OUT.is_file() or OUT.stat().st_size == 0:
            sys.stderr.write(result.stderr[-2000:] + "\n")
            sys.exit("Chromium produced no PDF")

    print(f"wrote {OUT.relative_to(REPO)}  ({OUT.stat().st_size / 1024:.0f} KB)")
    print(f"  from {SRC.relative_to(REPO)}  ({len(text.split())} words)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
