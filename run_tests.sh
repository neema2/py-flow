#!/usr/bin/env bash
# ── Test runner ─────────────────────────────────────────────────────
# 1. Kills ALL known service ports to avoid bind conflicts
# 2. Runs pytest, displays output live, captures to .test_runs/
#
# Usage:  ./run_tests.sh [pytest args...]
# Examples:
#   ./run_tests.sh                          # full suite
#   ./run_tests.sh tests/test_datacube.py   # single file
#   ./run_tests.sh -k "test_leaf"           # by name
set -uo pipefail

cd "$(dirname "$0")"

# ── Kill stale services ─────────────────────────────────────────────
# All ports from conftest.py + lakehouse services:
#   10000  = Deephaven streaming        18080  = MarketDataServer
#   19200  = QuestDB HTTP               19209  = QuestDB ILP
#   18922  = QuestDB PG                 5488   = Lakehouse embedded PG
#   8181   = Lakekeeper (Iceberg REST)  9002   = MinIO (S3)
echo "Cleaning up stale services..."
for port in 10000 18080 19200 19209 18922 5488 8181 9002; do
    lsof -ti :"$port" 2>/dev/null | xargs kill -9 2>/dev/null || true
done
pkill -9 -f postgres 2>/dev/null || true
pkill -9 -f lakekeeper 2>/dev/null || true
pkill -9 -f minio 2>/dev/null || true
sleep 1

# ── Run pytest with rolling history ────────────────────────────────
LOG_DIR=".test_runs"
mkdir -p "$LOG_DIR"
LOGFILE="$LOG_DIR/$(date +%Y%m%d_%H%M%S).txt"

if [ $# -eq 0 ]; then
    python3 -m pytest tests/ --tb=short 2>&1 | tee "$LOGFILE"
else
    python3 -m pytest "$@" 2>&1 | tee "$LOGFILE"
fi
RC=${PIPESTATUS[0]}

# Keep only the 10 most recent runs
ls -t "$LOG_DIR"/*.txt 2>/dev/null | tail -n +11 | xargs rm -f 2>/dev/null || true

echo ""
echo "Log saved: $LOGFILE"
echo "History:   ls -lt $LOG_DIR/"
exit $RC
