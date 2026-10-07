#!/usr/bin/env bash
# One-time setup: python env + Jupyter kernel for the practice notebooks.
# Safe to re-run.
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$PWD"
VENV="$ROOT/.venv"

if [ ! -x "$VENV/bin/python" ]; then
  echo "creating venv at .venv ..."
  if command -v uv >/dev/null 2>&1; then
    UV_CACHE_DIR="${UV_CACHE_DIR:-$ROOT/.uvcache}" uv venv --python 3.12 "$VENV"
    UV_CACHE_DIR="${UV_CACHE_DIR:-$ROOT/.uvcache}" uv pip install --python "$VENV/bin/python" \
      duckdb pandas pyarrow requests nbformat nbclient ipykernel "pyspark==4.0.1"
  else
    python3 -m venv "$VENV"
    "$VENV/bin/pip" install -q duckdb pandas pyarrow requests nbformat nbclient ipykernel "pyspark==4.0.1"
  fi
fi

# workspace-local kernelspec so `tools/run_notebooks.py` and Jupyter/VS Code use the right interpreter
mkdir -p "$ROOT/.jupyter/kernels/de-prep"
cat > "$ROOT/.jupyter/kernels/de-prep/kernel.json" <<EOF
{
  "argv": ["$VENV/bin/python", "-m", "ipykernel_launcher", "-f", "{connection_file}"],
  "display_name": "DE Prep (duckdb + pyspark)",
  "language": "python"
}
EOF

mkdir -p data/scratch
echo "ready: $("$VENV/bin/python" -c 'import duckdb, pandas, pyspark; print("duckdb", duckdb.__version__, "| pandas", pandas.__version__, "| pyspark", pyspark.__version__)')"
echo
echo "next: .venv/bin/python src/build_warehouse.py"
echo "open: notebooks/00_start_here.ipynb   (kernel: JUPYTER_PATH=\"\$PWD/.jupyter\" jupyter lab)"
