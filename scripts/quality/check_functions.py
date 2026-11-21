"""Enforce the repository's 15 physical lines per function, including closures."""

import argparse
import json
from pathlib import Path
import subprocess
import sys

from tree_sitter_language_pack import get_parser

LIMIT = 15
LANGUAGES = {
    ".go": "go", ".rs": "rust", ".py": "python", ".sh": "bash",
    ".js": "javascript", ".mjs": "javascript", ".cjs": "javascript",
    ".ts": "typescript", ".tsx": "tsx", ".java": "java",
}
FUNCTIONS = {
    "function_declaration", "method_declaration", "func_literal",
    "function_item", "closure_expression", "function_definition", "lambda",
    "function_expression", "generator_function_declaration", "generator_function",
    "arrow_function", "method_definition", "function", "constructor_declaration",
    "lambda_expression",
}
ROOT = Path(__file__).resolve().parents[2]


def source_files(paths):
    result = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        cwd=ROOT, check=True, capture_output=True,
    )
    files = sorted(set(result.stdout.decode().split("\0")) - {""})
    return [ROOT / name for name in files if selected(name, paths)]


def selected(name, paths):
    if Path(name).suffix not in LANGUAGES or not (ROOT / name).is_file():
        return False
    return not paths or any(name == path or name.startswith(path.rstrip("/") + "/") for path in paths)


def descendants(node):
    yield node
    for child in node.named_children:
        yield from descendants(child)


def function_name(node):
    name = node.child_by_field_name("name")
    return name.text.decode() if name else node.type


def scan_source(source, language, path):
    tree = get_parser(language).parse(source)
    functions = [node for node in descendants(tree.root_node) if node.type in FUNCTIONS]
    return [diagnostic(node, path) for node in functions if line_count(node) > LIMIT]


def line_count(node):
    return node.end_point.row - node.start_point.row + 1


def diagnostic(node, path):
    return {
        "path": str(path), "line": node.start_point.row + 1,
        "name": function_name(node), "lines": line_count(node),
    }


def check(paths):
    violations = []
    for path in source_files(paths):
        violations.extend(scan_source(path.read_bytes(), LANGUAGES[path.suffix], path.relative_to(ROOT)))
    return violations


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="*", help="Optional repository-relative paths")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable violations")
    return parser.parse_args()


def main():
    args = arguments()
    violations = check(args.paths)
    if args.json:
        print(json.dumps(violations, indent=2))
    else:
        for item in violations:
            print("{path}:{line}: {name} is {lines} lines (maximum 15)".format(**item))
        print(f"Function length: {len(violations)} violation(s)")
    return bool(violations)


if __name__ == "__main__":
    sys.exit(main())
