"""
Build notebooks/*.ipynb from the plain-python specs in notebooks/_build/.

    python tools/build_notebooks.py            # all specs
    python tools/build_notebooks.py 03 04      # only specs whose name starts with 03/04

Why specs?  Authoring cells as python strings keeps the content reviewable and diffable,
and every notebook gets the same tested preamble.
"""
from __future__ import annotations

import sys
from pathlib import Path

import nbformat as nbf

ROOT = Path(__file__).resolve().parent.parent
SPECS = ROOT / "notebooks" / "_build"
OUT = ROOT / "notebooks"

PREAMBLE = """import sys, pathlib
ROOT = next(p for p in [pathlib.Path.cwd(), *pathlib.Path.cwd().parents]
            if (p / "src" / "de_helpers.py").exists())
sys.path.insert(0, str(ROOT / "src"))

# Databricks Connect session via Databricks CLI authentication
from de_helpers import (
    q, q1, spark_q, assert_grain, tables, columns, explain, display, fs_ls,
    get_spark, get_dbutils, spark_read, plan_string, has_plan_operator
)
from pyspark.sql import functions as F, Window

spark = get_spark()
dbutils = get_dbutils()
"""

KERNEL = {
    "display_name": "DE Prep (duckdb + pyspark)",
    "language": "python",
    "name": "de-prep",
}


def build(spec_path: Path) -> Path:
    ns: dict = {}
    exec(compile(spec_path.read_text(), str(spec_path), "exec"), ns)  # noqa: S102
    cells = ns["CELLS"]
    title = ns.get("TITLE", spec_path.stem)

    nb = nbf.v4.new_notebook()
    nb.metadata = {
        "kernelspec": KERNEL,
        "language_info": {"name": "python", "version": "3.12"},
        "title": title,
    }
    if ns.get("PREAMBLE", True):
        nb.cells.append(nbf.v4.new_code_cell(PREAMBLE))
    for kind, source in cells:
        source = source.strip("\n")
        nb.cells.append(nbf.v4.new_markdown_cell(source) if kind == "md"
                        else nbf.v4.new_code_cell(source))
    nb.cells.insert(0, nbf.v4.new_markdown_cell(f"# {title}"))

    out_path = OUT / f"{spec_path.stem}.ipynb"
    nbf.write(nb, str(out_path))
    return out_path


def main() -> None:
    filters = sys.argv[1:]
    specs = sorted(SPECS.glob("*.py"))
    if filters:
        specs = [s for s in specs if any(s.stem.startswith(f) for f in filters)]
    if not specs:
        raise SystemExit(f"no specs found in {SPECS}")
    for spec in specs:
        path = build(spec)
        nb = nbf.read(str(path), as_version=4)
        print(f"built {path.relative_to(ROOT)}  ({len(nb.cells)} cells)")


if __name__ == "__main__":
    main()
