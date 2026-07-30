#!/bin/bash
# Portioned backfill driver. Usage: run_backfill.sh <polymarket|kalshi>
#
# Loops the venue's ingest module in bounded portions: each portion runs in
# a fresh process (exit 3 = more work, 0 = venue complete), so memory is
# fully returned to the OS between portions. A watchdog kills any portion
# whose RSS crosses the limit before it can push the machine into swap.
set -u
cd "$(dirname "$0")/.."

VENUE=$1
LOG="data/logs/${VENUE}.log"
RSS_LIMIT_KB=$((2 * 1024 * 1024))
FAILS=0
mkdir -p data/logs

while :; do
  PYTHONPATH=src .venv/bin/python -u -m "uindex.ingest.${VENUE}" >> "$LOG" 2>&1 &
  PID=$!
  while kill -0 "$PID" 2>/dev/null; do
    RSS=$(ps -o rss= -p "$PID" | tr -d ' ')
    if [ "${RSS:-0}" -gt "$RSS_LIMIT_KB" ]; then
      echo "watchdog: portion rss ${RSS}KB over limit, killing" >> "$LOG"
      kill "$PID"
    fi
    sleep 30
  done
  wait "$PID"
  case $? in
    0) echo "${VENUE} backfill complete" >> "$LOG"; exit 0 ;;
    3) FAILS=0 ;;
    *) FAILS=$((FAILS + 1))
       if [ "$FAILS" -ge 3 ]; then
         echo "${VENUE}: 3 consecutive portion failures, stopping" >> "$LOG"
         exit 1
       fi
       echo "${VENUE}: portion failed (attempt ${FAILS}/3), retrying in 60s" >> "$LOG"
       sleep 60 ;;
  esac
done
