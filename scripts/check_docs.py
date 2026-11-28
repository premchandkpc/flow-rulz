#!/usr/bin/env python3
"""Documentation checker for FlowRulz.

Checks (see docs/STANDARDS.md for the rules these enforce):

  links     every relative Markdown link and anchor resolves
  meta      every doc under docs/ (except the exempt list) has front matter
            with the required keys and valid values
  orphans   every doc is reachable from docs/README.md or a folder README
  mermaid   every ```mermaid fence is non-empty and has a known diagram type
  banned    no placeholder markers (TBD/FIXME/lorem) outside templates

Usage:
  python3 scripts/check_docs.py              # all checks, exit 1 on errors
  python3 scripts/check_docs.py --only links # one check
  python3 scripts/check_docs.py --warn-meta  # meta problems are warnings

Standard library only so it runs in CI without installs.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"

# Root-level docs that follow the same link rules but not the front-matter rules.
ROOT_DOCS = ["README.md", "AGENTS.md", "CLAUDE.md", "SECURITY.md"]

REQUIRED_META = ("title", "type", "status", "owner", "last_verified")
TYPES = {
    "index", "reference", "guide", "runbook", "adr", "spec", "plan",
    "report", "proposal", "template", "example", "diagram-index",
}
STATUSES = {"draft", "current", "proposed", "accepted", "superseded", "deprecated", "historical"}

# Files that are intentionally free-form: generated, templates carry placeholder
# front matter, or they are pure indexes with their own contract.
META_EXEMPT_PARTS = {"diagrams", "templates"}
META_EXEMPT_NAMES = set()

MERMAID_TYPES = (
    "flowchart", "graph", "sequenceDiagram", "classDiagram", "stateDiagram",
    "stateDiagram-v2", "erDiagram", "gantt", "journey", "pie", "C4Context",
    "mindmap", "timeline", "gitGraph", "quadrantChart", "requirementDiagram",
    "sankey-beta", "xychart-beta", "block-beta", "architecture-beta",
)
MERMAID_HEADER = re.compile(r"^(?:%%.*\n|---\n.*?\n---\n|\s*\n)*", re.S)

LINK_RE = re.compile(r"(?<!\!)\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
IMG_RE = re.compile(r"!\[[^\]]*\]\(([^)\s]+)")
HEADING_RE = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$", re.M)
FENCE_RE = re.compile(r"^(```|~~~).*?^\1", re.M | re.S)
BANNED_RE = re.compile(r"\b(TBD|FIXME|XXX|lorem ipsum)\b", re.I)


def slugify(heading: str) -> str:
    """GitHub-style heading anchor."""
    text = re.sub(r"`([^`]*)`", r"\1", heading)
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = text.strip().lower()
    text = re.sub(r"[^\w\- ]", "", text)
    return text.replace(" ", "-")


def strip_fences(text: str) -> str:
    return FENCE_RE.sub("", text)


def anchors_of(path: Path, cache: dict[Path, set[str]]) -> set[str]:
    if path in cache:
        return cache[path]
    text = strip_fences(path.read_text(encoding="utf-8", errors="replace"))
    seen: dict[str, int] = {}
    out: set[str] = set()
    for _, title in HEADING_RE.findall(text):
        slug = slugify(title)
        n = seen.get(slug, 0)
        out.add(slug if n == 0 else f"{slug}-{n}")
        seen[slug] = n + 1
    cache[path] = out
    return out


def parse_front_matter(text: str):
    if not text.startswith("---\n"):
        return None, text
    end = text.find("\n---", 4)
    if end == -1:
        return None, text
    block = text[4:end]
    body = text[end + 4:].lstrip("\n")
    meta: dict[str, str] = {}
    for line in block.splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        m = re.match(r"^([A-Za-z_][\w-]*):\s*(.*)$", line)
        if m:
            meta[m.group(1)] = m.group(2).strip().strip("\"'")
    return meta, body


def doc_files() -> list[Path]:
    files = sorted(DOCS.rglob("*.md"))
    files += [ROOT / n for n in ROOT_DOCS if (ROOT / n).exists()]
    return files


UNVERIFIED: list[str] = []


def check_links(files, errors):
    cache: dict[Path, set[str]] = {}
    for f in files:
        text = strip_fences(f.read_text(encoding="utf-8", errors="replace"))
        for target in LINK_RE.findall(text) + IMG_RE.findall(text):
            if re.match(r"^[a-z][a-z0-9+.-]*:", target, re.I) or target.startswith("//"):
                continue  # external or mailto
            path_part, _, frag = target.partition("#")
            path_part = unquote(path_part)
            dest = f if path_part == "" else (f.parent / path_part).resolve()
            rel = f.relative_to(ROOT)
            if not dest.exists():
                errors.append(f"{rel}: broken link -> {target}")
                continue
            if frag and dest.suffix == ".md" and dest.is_file():
                if unquote(frag).lower() not in anchors_of(dest, cache):
                    errors.append(f"{rel}: missing anchor -> {target}")


def check_meta(files, errors):
    for f in files:
        if f.parent == ROOT or f.name == "README.md" and f.parent == DOCS:
            pass
        if f.is_relative_to(ROOT) and f.parent == ROOT:
            continue
        rel = f.relative_to(ROOT)
        if any(p in META_EXEMPT_PARTS for p in rel.parts) or f.name in META_EXEMPT_NAMES:
            continue
        meta, _ = parse_front_matter(f.read_text(encoding="utf-8", errors="replace"))
        if meta is None:
            errors.append(f"{rel}: missing front matter")
            continue
        for key in REQUIRED_META:
            if not meta.get(key):
                errors.append(f"{rel}: front matter missing '{key}'")
        if meta.get("type") and meta["type"] not in TYPES:
            errors.append(f"{rel}: unknown type '{meta['type']}'")
        if meta.get("status") and meta["status"] not in STATUSES:
            errors.append(f"{rel}: unknown status '{meta['status']}'")
        lv = meta.get("last_verified", "")
        if lv == "never":
            UNVERIFIED.append(str(rel))
        elif lv and not re.match(r"^\d{4}-\d{2}-\d{2}(\s+@\s+[0-9a-f]{7,40})?$", lv):
            errors.append(f"{rel}: last_verified must be 'YYYY-MM-DD', 'YYYY-MM-DD @ <sha>' or 'never'")
        if meta.get("status") == "superseded" and not meta.get("superseded_by"):
            errors.append(f"{rel}: status superseded requires superseded_by")


def check_orphans(files, errors):
    reach: set[Path] = set()
    queue = [DOCS / "README.md"]
    while queue:
        cur = queue.pop()
        if cur in reach or not cur.exists():
            continue
        reach.add(cur)
        text = strip_fences(cur.read_text(encoding="utf-8", errors="replace"))
        for target in LINK_RE.findall(text):
            if re.match(r"^[a-z][a-z0-9+.-]*:", target, re.I):
                continue
            p = unquote(target.partition("#")[0])
            if not p:
                continue
            dest = (cur.parent / p).resolve()
            if dest.is_dir():
                dest = dest / "README.md"
            if dest.suffix == ".md":
                queue.append(dest)
    for f in files:
        if f.is_relative_to(DOCS) and f.resolve() not in reach:
            errors.append(f"{f.relative_to(ROOT)}: orphan (not reachable from docs/README.md)")


def check_mermaid(files, errors):
    fence = re.compile(r"^```mermaid\s*\n(.*?)^```", re.M | re.S)
    for f in files:
        for m in fence.finditer(f.read_text(encoding="utf-8", errors="replace")):
            body = MERMAID_HEADER.sub("", m.group(1), count=1).strip()
            first = body.split(None, 1)[0] if body else ""
            if not body:
                errors.append(f"{f.relative_to(ROOT)}: empty mermaid block")
            elif first not in MERMAID_TYPES:
                errors.append(f"{f.relative_to(ROOT)}: unknown mermaid diagram type '{first}'")
    for f in sorted((DOCS / "diagrams").glob("*.mmd")):
        body = MERMAID_HEADER.sub("", f.read_text(encoding="utf-8"), count=1).strip()
        first = body.split(None, 1)[0] if body else ""
        if first not in MERMAID_TYPES:
            errors.append(f"{f.relative_to(ROOT)}: unknown mermaid diagram type '{first or '<empty>'}'")


def check_banned(files, errors):
    for f in files:
        rel = f.relative_to(ROOT)
        if "templates" in rel.parts or f.name.endswith("-template.md") or rel.as_posix() == "docs/STANDARDS.md":
            continue
        text = strip_fences(f.read_text(encoding="utf-8", errors="replace"))
        for i, line in enumerate(text.splitlines(), 1):
            if BANNED_RE.search(line):
                errors.append(f"{rel}:{i}: placeholder marker: {line.strip()[:80]}")


CHECKS = {
    "links": check_links,
    "meta": check_meta,
    "orphans": check_orphans,
    "mermaid": check_mermaid,
    "banned": check_banned,
}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--only", choices=sorted(CHECKS), action="append")
    ap.add_argument("--warn-meta", action="store_true", help="report meta problems without failing")
    args = ap.parse_args()

    files = doc_files()
    selected = args.only or list(CHECKS)
    failed = False
    for name in selected:
        errs: list[str] = []
        CHECKS[name](files, errs)
        label = "WARN" if (name == "meta" and args.warn_meta) else "ERROR"
        for e in errs:
            print(f"{label} [{name}] {e}")
        if errs and label == "ERROR":
            failed = True
        print(f"-- {name}: {len(errs)} problem(s)")
    if UNVERIFIED:
        print(f"verification backlog: {len(UNVERIFIED)} doc(s) have last_verified: never")
    print(f"checked {len(files)} markdown files")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
