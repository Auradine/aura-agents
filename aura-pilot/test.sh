#!/bin/zsh
# test.sh - Automated test for README instructions
# 1. Build and start Docker containers (including client)
echo "[INFO] Building and starting Docker containers (questdb, server, client)..."
docker-compose build || { echo "[FAIL] docker-compose build failed"; exit 1; }
docker-compose up -d || { echo "[FAIL] docker-compose up failed"; exit 1; }

# 2. Wait for containers to be healthy and client to finish
# Wait for QuestDB to be available on /index.html (should return 200)
MAX_WAIT=120
WAITED=0

echo "[INFO] Waiting for QuestDB to be available on http://localhost:9000/index.html..."
while true; do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:9000/index.html)
  echo "[DEBUG] HTTP status from QuestDB /index.html: $STATUS (waited $WAITED seconds)"
  if [ "$STATUS" = "200" ]; then
    break
  fi
  sleep 2
  WAITED=$((WAITED+2))
  if [ $WAITED -ge $MAX_WAIT ]; then
    echo "[FAIL] QuestDB did not become available in $MAX_WAIT seconds."
    docker-compose logs questdb
    exit 1
  fi
done
echo "[INFO] QuestDB is up."

# Wait for telemetry data to stream in for a short period
STREAM_WAIT=10

echo "[INFO] Waiting $STREAM_WAIT seconds for continuous telemetry data to stream in..."
sleep $STREAM_WAIT

echo "[INFO] Telemetry client finished. Running SQL query against QuestDB..."
# 3. Run a simple SQL query to check if any data exists in switch_telemetry
QUERY="SELECT COUNT(*) AS row_count FROM switch_telemetry;"

RESULT=$(curl -s -G --data-urlencode "query=$QUERY" http://localhost:9000/exec)

# 4. Check if at least one row is present in the result
ROW_COUNT=$(echo "$RESULT" | grep -o '[0-9]\+' | head -1)
if [ "$ROW_COUNT" -ge 1 ]; then
    echo "[PASS] Telemetry data is present in QuestDB ($ROW_COUNT rows)."
    docker-compose down -v
    exit 0
else
    echo "[FAIL] No telemetry data found in QuestDB."
    docker-compose down -v
    exit 1
fi
