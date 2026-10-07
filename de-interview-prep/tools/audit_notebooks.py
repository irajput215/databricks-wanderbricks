"""
Style/coverage audit for the drill notebooks.

    .venv/bin/python tools/audit_notebooks.py

Checks every question block has: a plan markdown, a `# YOUR TURN` stub and a `# SOLUTION` cell,
and that the markdown stays short (the user asked for no bloated reading material).
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
NOTEBOOKS = ROOT / "notebooks"
MAX_MD_LINES = 14          # per question block, generous headroom over the 10-line target
QUESTION_RE = re.compile(r"^#{3,4}\s*(Q\s?\d{1,2}\b.*)$", re.MULTILINE)

failures: list[str] = []


def main() -> None:
    total_q = total_turn = total_sol = 0
    print(f"{'notebook':<40} {'Q':>3} {'turn':>5} {'sol':>4} {'max md lines':>13}")
    for path in sorted(NOTEBOOKS.glob("*.ipynb")):
        if path.name.startswith("_"):
            continue
        nb = json.loads(path.read_text())
        cells = [("".join(c["source"]), c["cell_type"]) for c in nb["cells"]]
        questions = [m.group(1).strip() for src, kind in cells if kind == "markdown"
                     for m in QUESTION_RE.finditer(src)]
        turns = sum(1 for src, kind in cells if kind == "code" and "# YOUR TURN" in src)
        sols = sum(1 for src, kind in cells if kind == "code" and "# SOLUTION" in src)
        total_q += len(questions)
        total_turn += turns
        total_sol += sols

        # longest markdown block
        md_lines = max((len(src.strip().splitlines()) for src, kind in cells if kind == "markdown"),
                       default=0)
        print(f"{path.name:<40} {len(questions):>3} {turns:>5} {sols:>4} {md_lines:>13}")

        # drill notebooks must have a stub + solution per question; investigation notebooks
        # (Q49/Q50 style) are allowed to be query-driven instead
        strict = bool(questions) and not path.stem.startswith(("06_", "09_", "10_", "11_"))
        if strict and turns < len(questions):
            failures.append(f"{path.name}: {len(questions)} questions but only {turns} YOUR TURN stubs")
        if strict and sols < len(questions):
            failures.append(f"{path.name}: {len(questions)} questions but only {sols} SOLUTION cells")
        if questions and md_lines > MAX_MD_LINES:
            failures.append(f"{path.name}: a markdown cell has {md_lines} lines (>{MAX_MD_LINES})")

    print(f"\ntotal: {total_q} questions, {total_turn} stubs, {total_sol} solutions")
    if failures:
        print("\naudit findings:")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    print("audit clean")


if __name__ == "__main__":
    main()
