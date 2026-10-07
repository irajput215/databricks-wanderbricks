"""
Execute notebooks end-to-end and report failures.  Used to verify every drill actually runs.

    python tools/run_notebooks.py                 # all notebooks
    python tools/run_notebooks.py 03 04           # prefix filter
    python tools/run_notebooks.py --save          # keep executed copies in notebooks/_executed/
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
os.environ["JUPYTER_PATH"] = str(ROOT / ".jupyter") + os.pathsep + os.environ.get("JUPYTER_PATH", "")

import nbformat as nbf  # noqa: E402
from nbclient import NotebookClient  # noqa: E402

NOTEBOOKS = ROOT / "notebooks"
EXECUTED = NOTEBOOKS / "_executed"
KERNEL = "de-prep"


def run(path: Path, save: bool = False, timeout: int = 600):
    nb = nbf.read(str(path), as_version=4)
    client = NotebookClient(nb, timeout=timeout, kernel_name=KERNEL,
                            resources={"metadata": {"path": str(NOTEBOOKS)}},
                            allow_errors=False)
    started = time.time()
    client.execute()
    elapsed = time.time() - started
    if save:
        EXECUTED.mkdir(exist_ok=True)
        nbf.write(nb, str(EXECUTED / path.name))
    return elapsed, nb


def main() -> None:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    save = "--save" in sys.argv
    files = sorted(p for p in NOTEBOOKS.glob("*.ipynb") if not p.name.startswith("_"))
    if args:
        files = [p for p in files if any(p.stem.startswith(a) for a in args)]
    failures = []
    for path in files:
        try:
            elapsed, _ = run(path, save=save)
            print(f"PASS  {path.name:<52} {elapsed:6.1f}s")
        except Exception as exc:  # noqa: BLE001
            msg = str(exc).splitlines()
            failures.append((path.name, msg))
            print(f"FAIL  {path.name:<52} {msg[-1][:160]}")
    print()
    print(f"{len(files) - len(failures)}/{len(files)} notebooks executed cleanly")
    if failures:
        print("\ndetails:")
        for name, msg in failures:
            print(f"\n--- {name} ---")
            print("\n".join(msg[-25:]))
        sys.exit(1)


if __name__ == "__main__":
    main()
