#!/bin/bash
# scripts/run_nightly_tests.sh
# Fire & Forget robust test rig for py-flow
#
# Usage (run in background and detach from terminal):
#   nohup ./scripts/run_nightly_tests.sh > /dev/null 2>&1 &
set -u

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
export PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"
export PYTHONPATH="$PROJECT_ROOT"

LOG_DIR="${PROJECT_ROOT}/test_logs"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${LOG_DIR}/nightly_run_${TIMESTAMP}.log"
SUMMARY_FILE="${LOG_DIR}/nightly_summary_${TIMESTAMP}.txt"

echo "==========================================================" > "$SUMMARY_FILE"
echo "Py-Flow Robust Test Run - ${TIMESTAMP}" >> "$SUMMARY_FILE"
echo "Full Logs: ${LOG_FILE}" >> "$SUMMARY_FILE"
echo "==========================================================" >> "$SUMMARY_FILE"

# 1. Purge Hung Processes, Ports, and Shared Memory
echo "=> [1/4] Terminating hanging pytest, sed, and python processes..." | tee -a "$LOG_FILE"
pkill -f "pytest" || true
pkill -f "verify_sql.py" || true
pkill -f "sed -i" || true

echo "   Cleaning up all stale test service ports (10000+, 9000+)..." | tee -a "$LOG_FILE"
for port in 10000 8765 9200 9209 8922 5490 8183 9004 9005 9102 9103 8050 \
            10200 8965 9400 9409 9122 5690 8383 9204 9205 9302 9303; do
    lsof -ti :"$port" 2>/dev/null | xargs kill -9 2>/dev/null || true
done
pkill -9 -f postgres 2>/dev/null || true
pkill -9 -f lakekeeper 2>/dev/null || true
pkill -9 -f minio 2>/dev/null || true

echo "   Sweeping stale SysV shared memory segments..." | tee -a "$LOG_FILE"
for seg in $(ipcs -m 2>/dev/null | awk '/^m / || /^0x/ {print $2}' | grep -E '^[0-9]+$'); do
    ipcrm -m "$seg" 2>/dev/null || true
done
sleep 2 

# 2. Restart Docker Environments (Deephaven / PGServer emulation)
echo "=> [2/4] Restarting backing Docker services to prevent memory leaks..." | tee -a "$LOG_FILE"
if docker info > /dev/null 2>&1; then
    echo "   Stopping running containers..." | tee -a "$LOG_FILE"
    docker stop $(docker ps -q) 2>/dev/null || true
    
    echo "   Restarting stopped containers..." | tee -a "$LOG_FILE"
    # We cycle all recent containers to ensure fresh DB state for tests
    docker start $(docker ps -aq) 2>/dev/null || true
    
    # Wait for databases to wake
    sleep 5
else
    echo "   Docker daemon not detected, bypassing restart." | tee -a "$LOG_FILE"
fi

# 3. Test Runner Configuration
TIMEOUT_SECS=300
TEST_FILES=$(find tests -name "test_*.py" | sort)
NUM_TESTS=$(echo "$TEST_FILES" | wc -w)

echo "=> [3/4] Discovered ${NUM_TESTS} test files." | tee -a "$LOG_FILE"
echo "         Running in strict isolation with ${TIMEOUT_SECS}s timeout each..." | tee -a "$LOG_FILE"

# Architecture detection for Pytest forking (crucial for pydeephaven gRPC sockets on ARM)
PYTEST_ARGS=""
# if [[ "$(uname -m)" == "aarch64" ]] || [[ "$(uname -m)" == "arm64" ]]; then
#     PYTEST_ARGS="--forked"
#     echo "         Detected ARM64: Enabling --forked isolation to protect gRPC." | tee -a "$LOG_FILE"
# fi

run_test() {
    local target_file="$1"
    echo "" | tee -a "$LOG_FILE"
    echo "----------------------------------------------------------" | tee -a "$LOG_FILE"
    echo "Running: ${target_file}" | tee -a "$LOG_FILE"
    echo "----------------------------------------------------------" | tee -a "$LOG_FILE"
    
    # We execute each file completely independently in its own process
    # with a strict timeout so a hang does not freeze the nightly run.
    timeout "${TIMEOUT_SECS}s" python3 -m pytest "$target_file" -v $PYTEST_ARGS >> "$LOG_FILE" 2>&1
    local exit_code=$?
    
    if [ $exit_code -eq 124 ]; then
        echo "❌ TIMEOUT (${TIMEOUT_SECS}s) - ${target_file}" | tee -a "$SUMMARY_FILE"
    elif [ $exit_code -eq 0 ]; then
        echo "✅ PASS - ${target_file}" | tee -a "$SUMMARY_FILE"
    else
        echo "❌ FAIL (Code ${exit_code}) - ${target_file}" | tee -a "$SUMMARY_FILE"
        # Print trailing 3 lines of log failure context to the summary for quick dashboarding
        echo "     ...Tail of trace:..." >> "$SUMMARY_FILE"
        tail -n 3 "$LOG_FILE" | sed 's/^/         /' >> "$SUMMARY_FILE"
    fi
}

for file in $TEST_FILES; do
    run_test "$file"
done

# 4. Success Finalizer
echo "=> [4/4] Nightly Run Completed at $(date +"%Y%m%d_%H%M%S")" | tee -a "$LOG_FILE"
echo "==========================================================" >> "$SUMMARY_FILE"
echo "Run Summary Complete." >> "$SUMMARY_FILE"

# Print final status
cat "$SUMMARY_FILE"
