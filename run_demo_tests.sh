#!/usr/bin/env bash
# ── Demo mirror test runner ─────────────────────────────────────────
# Runs ONLY the test_demo_* files — the mirror tests for each demo.
#
# These tests require full platform infrastructure (Deephaven JVM,
# QuestDB, MinIO, PostgreSQL, Gemini API) and take longer to run.
#
# Usage:  ./run_demo_tests.sh [pytest args...]
# Examples:
#   ./run_demo_tests.sh                                    # all demo tests
#   ./run_demo_tests.sh tests/test_demo_trading.py         # single demo
#   ./run_demo_tests.sh -k "test_demo_bridge"              # by name
set -uo pipefail

# Port offset so demos can run in parallel with main tests (./run_tests.sh).
# Must be >= 200 because QuestDB binds a hidden port at http_port+3.
export PORT_OFFSET=200

cd "$(dirname "$0")"

# ── Kill stale services (skipped when SKIP_CLEANUP=1 for parallel runs) ─
if [ "${SKIP_CLEANUP:-}" != "1" ]; then
    # Offset ports (base + 200):
    echo "Cleaning up stale services (offset ports)..."
    for port in 10200 8965 9400 9409 9122 5690 8383 9204 9205 9302 9303; do
        lsof -ti :"$port" 2>/dev/null | xargs kill -9 2>/dev/null || true
    done
    pkill -9 -f postgres 2>/dev/null || true
    pkill -9 -f lakekeeper 2>/dev/null || true
    pkill -9 -f minio 2>/dev/null || true

    for seg in $(ipcs -m 2>/dev/null | awk '/^m / || /^0x/ {print $2}' | grep -E '^[0-9]+$'); do
        ipcrm -m "$seg" 2>/dev/null || true
    done
    sleep 1
fi

# ── Run demo tests ─────────────────────────────────────────────────
LOG_DIR="${LOG_DIR:-.test_runs}"
mkdir -p "$LOG_DIR"
LOGFILE="$LOG_DIR/$(date +%Y%m%d_%H%M%S)_demo.txt"

# ── Architecture detection for Pytest forking ─────────────────────────
PYTEST_ARGS=""
if [[ "$(uname -m)" == "aarch64" ]] || [[ "$(uname -m)" == "arm64" ]]; then
    # Run tests in forked subprocesses to ensure pydeephaven gRPC sockets 
    # are cleanly destroyed between tests and never leak stale connections.
    PYTEST_ARGS="--forked"
fi

if [ $# -eq 0 ]; then
    python3 -m pytest tests/test_demo_*.py -v $PYTEST_ARGS --durations=0 2>&1 | tee "$LOGFILE"
else
    python3 -m pytest "$@" $PYTEST_ARGS --durations=0 2>&1 | tee "$LOGFILE"
fi
RC=${PIPESTATUS[0]}

# Keep only the 10 most recent runs
ls -t "$LOG_DIR"/*.txt 2>/dev/null | tail -n +11 | xargs rm -f 2>/dev/null || true

echo ""
echo "Log saved: $LOGFILE"
echo "History:   ls -lt $LOG_DIR/"
exit $RC
