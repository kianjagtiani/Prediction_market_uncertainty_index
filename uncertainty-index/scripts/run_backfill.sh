#!/bin/bash
# Portioned backfill driver. Usage: run_backfill.sh <polymarket|kalshi|polymarket_volume>
#
# Loops the venue's ingest module in bounded portions: each portion runs in
# a fresh process (exit 3 = more work, 0 = venue complete), so memory is
# fully returned to the OS between portions. A watchdog kills any portion
# whose RSS crosses the limit before it can push the machine into swap;
# killed portions checkpointed their progress, so a kill (exit 143) is
# treated as progress, not failure.
#
# Exit 3 and 143 reset the failure counter, so "progress" has to be
# verified, not assumed: a portion that waits on a file that never appears,
# or one the watchdog kills before it can write anything, would otherwise
# loop forever with no failure and no alert. STALL_LIMIT consecutive
# progress-coded exits that leave the venue's data directory byte-identical
# abort the run.
set -u
cd "$(dirname "$0")/.."

VENUE=$1
LOG="data/logs/${VENUE}.log"
# Lock the DATA directory, not the module: polymarket and
# polymarket_volume are separate modules writing data/raw/polymarket, and
# markets.parquet being os.replace'd mid-token-map invalidates the token
# map's row cursor.
DATA_DIR="data/raw/${VENUE%_volume}"
LOCK="data/logs/${VENUE%_volume}.lock"
RSS_LIMIT_KB=$((2 * 1024 * 1024))
STALL_LIMIT=10
FAILS=0
STALLS=0
LAST_TOKEN=""
mkdir -p data/logs

progress_token() {
  find "$DATA_DIR" -type f 2>/dev/null | sort | xargs -r ls -ld 2>/dev/null | cksum
}

if ! mkdir "$LOCK" 2>/dev/null; then
  echo "${VENUE}: already running (rmdir ${LOCK} if stale)" >&2
  exit 1
fi
PID=""
trap 'kill "$PID" 2>/dev/null; rmdir "$LOCK" 2>/dev/null' EXIT

while :; do
  PYTHONPATH=src .venv/bin/python -u -m "uindex.ingest.${VENUE}" >> "$LOG" 2>&1 &
  PID=$!
  while kill -0 "$PID" 2>/dev/null; do
    RSS=$(ps -o rss= -p "$PID" | tr -d ' ')
    if [ "${RSS:-0}" -gt "$RSS_LIMIT_KB" ]; then
      echo "watchdog: portion rss ${RSS}KB over limit, killing" >> "$LOG"
      kill "$PID" 2>/dev/null
    fi
    sleep 30
  done
  wait "$PID"
  STATUS=$?

  TOKEN=$(progress_token)
  if [ "$TOKEN" = "$LAST_TOKEN" ]; then
    STALLS=$((STALLS + 1))
  else
    STALLS=0
  fi
  LAST_TOKEN="$TOKEN"

  case $STATUS in
    0) echo "${VENUE} backfill complete" >> "$LOG"; exit 0 ;;
    3|143) FAILS=0 ;;
    *) FAILS=$((FAILS + 1))
       if [ "$FAILS" -ge 3 ]; then
         echo "${VENUE}: 3 consecutive portion failures, stopping" >> "$LOG"
         exit 1
       fi
       echo "${VENUE}: portion failed (attempt ${FAILS}/3), retrying in 60s" >> "$LOG"
       sleep 60 ;;
  esac

  if [ "$STALLS" -ge "$STALL_LIMIT" ]; then
    echo "${VENUE}: ${STALLS} portions with no change in ${DATA_DIR}, stopping" >> "$LOG"
    exit 1
  fi
done
