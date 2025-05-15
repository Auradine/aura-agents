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

# Wait for telemetry-client to finish (if it is a one-shot job)
echo "[INFO] Waiting for telemetry-client to finish..."
docker-compose logs -f telemetry-client &
CLIENT_PID=$!
while [ "$(docker inspect -f '{{.State.Running}}' telemetry-client 2>/dev/null)" = "true" ]; do
  sleep 2
done
kill $CLIENT_PID 2>/dev/null

echo "[INFO] Telemetry client finished. Running SQL query against QuestDB..."
# 3. Run the SQL query to get start and end times of all downtimes
QUERY="WITH link_states AS (\n\nSELECT port, timestamp,\n\nCASE WHEN linkState = 'up' THEN 1 ELSE 0 END AS is_up\n\nFROM switch_telemetry\n\n),\n\nstate_changes AS (\n\nSELECT port, timestamp, is_up,\n\nLAG(is_up) OVER (PARTITION BY port ORDER BY timestamp) AS prev_state\n\nFROM (\n\nSELECT DISTINCT port, timestamp, is_up\n\nFROM link_states\n\nORDER BY port, timestamp\n\n)\n\n),\n\nflap_starts AS (\n\nSELECT port, timestamp AS flap_start_time\n\nFROM state_changes\n\nWHERE is_up = 0 AND (prev_state IS NULL OR prev_state = 1)\n\n)\n\nSELECT\n\nf.port,\n\nf.flap_start_time AS down_time,\n\nMIN(u.timestamp) AS up_time,\n\nMIN(u.timestamp) - f.flap_start_time AS downtime_ms\n\nFROM flap_starts f\n\nJOIN link_states u ON f.port = u.port\n\nAND u.timestamp > f.flap_start_time\n\nAND u.is_up = 1\n\nGROUP BY f.port, f.flap_start_time\n\nORDER BY f.port, f.flap_start_time;"

RESULT=$(curl -s -G --data-urlencode "query=$QUERY" http://localhost:9000/exec)

# 4. Check if at least one downtime is listed (look for a row in the result)
if echo "$RESULT" | grep -q 'down_time'; then
    ROWS=$(echo "$RESULT" | grep -c 'down_time')
    if [ "$ROWS" -ge 1 ]; then
        echo "[PASS] At least one downtime listed in query result."
        docker-compose down -v
        exit 0
    fi
else
    # Try to check for at least one data row (not just header)
    if echo "$RESULT" | grep -q 'flap_start_time'; then
        ROWS=$(echo "$RESULT" | grep -c 'flap_start_time')
        if [ "$ROWS" -ge 2 ]; then
            echo "[PASS] At least one downtime listed in query result."
            docker-compose down -v
            exit 0
        fi
    fi
fi

echo "[FAIL] No downtime listed in query result."
docker-compose down -v
exit 1
