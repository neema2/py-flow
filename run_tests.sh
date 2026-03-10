#!/usr/bin/env bash
# ── Test runner ─────────────────────────────────────────────────────
# 1. Kills ALL known service ports to avoid bind conflicts
# 2. Runs pytest, displays output live, captures to .test_runs/
#
# Excludes test_demo_* files by default (use run_demo_tests.sh for those).
#
# Usage:  ./run_tests.sh [pytest args...]
# Examples:
#   ./run_tests.sh                          # unit + integration (no demos)
#   ./run_tests.sh tests/test_datacube.py   # single file
#   ./run_tests.sh -k "test_leaf"           # by name
set -uo pipefail

cd "$(dirname "$0")"

# ── Kill stale services (skipped when SKIP_CLEANUP=1 for parallel runs) ─
if [ "${SKIP_CLEANUP:-}" != "1" ]; then
    # All ports from conftest.py session fixtures (base, no offset):
    echo "Cleaning up stale services..."
    for port in 10000 8765 9200 9209 8922 5490 8183 9004 9005 9102 9103 8050; do
        lsof -ti :"$port" 2>/dev/null | xargs kill -9 2>/dev/null || true
    done
    pkill -9 -f postgres 2>/dev/null || true
    pkill -9 -f lakekeeper 2>/dev/null || true
    pkill -9 -f minio 2>/dev/null || true

    # Clean up stale System V shared memory segments left by SIGKILL'd postgres.
    for seg in $(ipcs -m 2>/dev/null | awk '/^m / || /^0x/ {print $2}' | grep -E '^[0-9]+$'); do
        ipcrm -m "$seg" 2>/dev/null || true
    done
    sleep 1
fi

# ── Run pytest with rolling history ────────────────────────────────
LOG_DIR=".test_runs"
mkdir -p "$LOG_DIR"
LOGFILE="$LOG_DIR/$(date +%Y%m%d_%H%M%S).txt"

# ── Architecture detection for Pytest forking ─────────────────────────
PYTEST_ARGS=""
if [[ "$(uname -m)" == "aarch64" ]] || [[ "$(uname -m)" == "arm64" ]]; then
    # Run tests in forked subprocesses to ensure pydeephaven gRPC sockets 
    # are cleanly destroyed between tests and never leak stale connections.
    PYTEST_ARGS="--forked"
fi

if [ $# -eq 0 ]; then
    python3 -m pytest tests/ -v $PYTEST_ARGS --durations=0 \
        --ignore-glob='tests/test_demo_*.py' 2>&1 | tee "$LOGFILE"
else
    # Automatically ensure --forked is used on ARM unless explicitly disabled
    python3 -m pytest "$@" $PYTEST_ARGS --durations=0 2>&1 | tee "$LOGFILE"
fi
RC=${PIPESTATUS[0]}

# Keep only the 10 most recent runs
ls -t "$LOG_DIR"/*.txt 2>/dev/null | tail -n +11 | xargs rm -f 2>/dev/null || true

echo ""
echo "Log saved: $LOGFILE"
echo "History:   ls -lt $LOG_DIR/"
exit $RC
