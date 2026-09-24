#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# Resolve repo root (parent of the notebooks/ directory this script lives in)
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

JUPYTER_VENV="$HOME/.jupyter-venv"
IJAVA_VERSION="1.3.0"
IJAVA_KERNEL_DIR="$JUPYTER_VENV/share/jupyter/kernels/java"

echo "==> Repo root: $REPO_ROOT"

# Check prerequisites
for cmd in java mvn python3 curl unzip; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "ERROR: '$cmd' not found. Please install it before running this script." >&2
        exit 1
    fi
done

# Build the project — installs JARs into ~/.m2 (no internet for the lib)
echo "==> Building zarr-java (skipping tests)..."
cd "$REPO_ROOT"
mvn install -DskipTests -q
echo "    Build done."

# Build IJAVA_CLASSPATH directly from ~/.m2 — no copying needed
echo "==> Resolving classpath from local Maven repo..."
CLASSPATH_FILE="$(mktemp)"
mvn dependency:build-classpath \
    -pl zarr-java-ome \
    -DincludeScope=runtime \
    -Dmdep.outputFile="$CLASSPATH_FILE" \
    -q
DEPS_CLASSPATH="$(cat "$CLASSPATH_FILE")"
rm -f "$CLASSPATH_FILE"

OME_JAR=$(find "$REPO_ROOT/zarr-java-ome/target" -name "zarr-java-ome-*.jar" \
    ! -name "*sources*" ! -name "*javadoc*" | head -1)
OME_JAR="$(cd "$(dirname "$OME_JAR")" && pwd)/$(basename "$OME_JAR")"

export IJAVA_CLASSPATH="$OME_JAR:$DEPS_CLASSPATH"
echo "    Classpath ready ($(echo "$IJAVA_CLASSPATH" | tr ':' '\n' | wc -l) entries)."

# Ensure Jupyter is installed in ~/.jupyter-venv
if [ ! -x "$JUPYTER_VENV/bin/jupyter" ]; then
    echo "==> Jupyter not found — creating venv and installing..."
    python3 -m venv "$JUPYTER_VENV"
    "$JUPYTER_VENV/bin/pip" install jupyter --quiet
    echo "    Jupyter installed."
fi

# Ensure IJava kernel is installed (one-time download from GitHub — only the tooling, not the library)
if [ ! -f "$IJAVA_KERNEL_DIR/kernel.json" ]; then
    echo "==> IJava kernel not found — downloading and installing (one-time)..."
    TMP_IJAVA="$(mktemp -d)"
    curl -sL "https://github.com/SpencerPark/IJava/releases/download/v${IJAVA_VERSION}/ijava-${IJAVA_VERSION}.zip" \
         -o "$TMP_IJAVA/ijava.zip"
    unzip -q "$TMP_IJAVA/ijava.zip" -d "$TMP_IJAVA"
    JUPYTER_DATA_DIR="$JUPYTER_VENV/share/jupyter" \
        "$JUPYTER_VENV/bin/python" "$TMP_IJAVA/install.py" --sys-prefix
    rm -rf "$TMP_IJAVA"
    echo "    IJava kernel installed."
fi

# Launch JupyterLab — opens directly into the notebooks/ subfolder
echo "==> Starting JupyterLab — press Ctrl+C to stop."
echo ""
JUPYTER_DATA_DIR="$JUPYTER_VENV/share/jupyter" \
    "$JUPYTER_VENV/bin/jupyter" lab "$SCRIPT_DIR/notebooks"
