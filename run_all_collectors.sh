#!/bin/bash

# Script to run all data collectors simultaneously
# Each collector runs in the background and logs to a separate file

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Determine Python executable (use venv if available)
if [ -f "$SCRIPT_DIR/.venv/bin/python" ]; then
    PYTHON_CMD="$SCRIPT_DIR/.venv/bin/python"
    echo "Using virtual environment: $PYTHON_CMD"
else
    PYTHON_CMD="python3"
    echo "Using system Python: $PYTHON_CMD"
fi

# Create logs directory if it doesn't exist
LOGS_DIR="logs"
mkdir -p "$LOGS_DIR"

# PID file to track running processes
PID_FILE="$LOGS_DIR/collectors.pid"

# Function to stop all collectors
stop_collectors() {
    if [ -f "$PID_FILE" ]; then
        echo "Stopping all collectors..."
        while read pid; do
            if kill -0 "$pid" 2>/dev/null; then
                kill "$pid"
                echo "Stopped process $pid"
            fi
        done < "$PID_FILE"
        rm "$PID_FILE"
        echo "All collectors stopped."
    else
        echo "No collectors are running (PID file not found)."
    fi
}

# Check if stop command is given
if [ "$1" == "stop" ]; then
    stop_collectors
    exit 0
fi

# Check if collectors are already running
if [ -f "$PID_FILE" ]; then
    echo "Warning: PID file exists. Checking if collectors are still running..."
    RUNNING=false
    while read pid; do
        if kill -0 "$pid" 2>/dev/null; then
            RUNNING=true
            break
        fi
    done < "$PID_FILE"

    if [ "$RUNNING" = true ]; then
        echo "Collectors are already running. Use './run_all_collectors.sh stop' to stop them first."
        exit 1
    else
        echo "Cleaning up stale PID file..."
        rm "$PID_FILE"
    fi
fi

# Create timestamp for log files
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# Log files for each collector
AVINOR_LOG="$LOGS_DIR/avinor_${TIMESTAMP}.log"
ENTUR_SIRI_ET_LOG="$LOGS_DIR/entur_siri_et_${TIMESTAMP}.log"
OSLOBYKSYKKEL_LOG="$LOGS_DIR/oslobysykkel_${TIMESTAMP}.log"
VEGVESEN_LOG="$LOGS_DIR/vegvesen_${TIMESTAMP}.log"

# PID storage
PIDS=()

echo "Starting all data collectors..."
echo "Logs will be saved in: $LOGS_DIR"
echo ""

# Start Avinor collector
echo "Starting Avinor collector..."
cd "$SCRIPT_DIR"
PYTHONPATH="$SCRIPT_DIR" $PYTHON_CMD -u avinor/avinor_collect.py > "$AVINOR_LOG" 2>&1 &
AVINOR_PID=$!
PIDS+=($AVINOR_PID)
echo "  Avinor PID: $AVINOR_PID -> $AVINOR_LOG"

# Start Entur SIRI ET collector (estimated timetable / delays)
echo "Starting Entur SIRI ET collector..."
cd "$SCRIPT_DIR"
PYTHONPATH="$SCRIPT_DIR" $PYTHON_CMD -u entur/entur_siri_et_collect.py > "$ENTUR_SIRI_ET_LOG" 2>&1 &
ENTUR_SIRI_ET_PID=$!
PIDS+=($ENTUR_SIRI_ET_PID)
echo "  Entur SIRI ET PID: $ENTUR_SIRI_ET_PID -> $ENTUR_SIRI_ET_LOG"

# Start Oslo Bysykkel collector
echo "Starting Oslo Bysykkel collector..."
cd "$SCRIPT_DIR"
PYTHONPATH="$SCRIPT_DIR" $PYTHON_CMD -u oslobysykkel/oslobysykkel_collect.py > "$OSLOBYKSYKKEL_LOG" 2>&1 &
OSLOBYKSYKKEL_PID=$!
PIDS+=($OSLOBYKSYKKEL_PID)
echo "  Oslo Bysykkel PID: $OSLOBYKSYKKEL_PID -> $OSLOBYKSYKKEL_LOG"

# Start Vegvesen collector
echo "Starting Vegvesen collector..."
cd "$SCRIPT_DIR"
PYTHONPATH="$SCRIPT_DIR" $PYTHON_CMD -u vegvesen/vegvesen_collect.py > "$VEGVESEN_LOG" 2>&1 &
VEGVESEN_PID=$!
PIDS+=($VEGVESEN_PID)
echo "  Vegvesen PID: $VEGVESEN_PID -> $VEGVESEN_LOG"

# Save PIDs to file
printf "%s\n" "${PIDS[@]}" > "$PID_FILE"

echo ""
echo "All collectors started successfully!"
echo ""
echo "To view logs in real-time:"
echo "  tail -f $AVINOR_LOG"
echo "  tail -f $ENTUR_SIRI_ET_LOG"
echo "  tail -f $OSLOBYKSYKKEL_LOG"
echo "  tail -f $VEGVESEN_LOG"
echo ""
echo "To stop all collectors:"
echo "  ./run_all_collectors.sh stop"
echo ""
