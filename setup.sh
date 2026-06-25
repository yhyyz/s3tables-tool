#!/bin/bash
# setup.sh - 一次性安装 PyIceberg 依赖到独立 venv
#
# 用法: bash setup.sh

set -e

VENV_DIR="${VENV_DIR:-/tmp/pyice}"

echo "=== Creating venv at $VENV_DIR ==="
python3 -m venv "$VENV_DIR"

echo "=== Installing pyiceberg + pyarrow ==="
# 优先用 uv (快 10 倍), 否则回退到 pip
if command -v uv &> /dev/null; then
    uv pip install --python "$VENV_DIR/bin/python" "pyiceberg[glue,s3fs]" pyarrow
else
    "$VENV_DIR/bin/pip" install --quiet "pyiceberg[glue,s3fs]" pyarrow
fi

echo ""
echo "=== Verifying install ==="
"$VENV_DIR/bin/python" -c "
import pyiceberg
import pyarrow
print(f'  pyiceberg: {pyiceberg.__version__}')
print(f'  pyarrow:   {pyarrow.__version__}')
"

echo ""
echo "✅ Setup complete. Python interpreter: $VENV_DIR/bin/python"
echo ""
echo "Next steps:"
echo "  1. Preview:   DRY_RUN=1 $VENV_DIR/bin/python add_event_time_partition.py"
echo "  2. Execute:   $VENV_DIR/bin/python add_event_time_partition.py"
echo "  3. Verify:    $VENV_DIR/bin/python verify_partition.py"
echo "  4. Rollback:  ROLLBACK=1 $VENV_DIR/bin/python add_event_time_partition.py"
