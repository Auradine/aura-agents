# Getting Started

To get started, open a terminal window. Make sure Docker is installed, and then run the following:
```
docker-compose build
docker-compose up
```

Wait for a few seconds to allow for the client to insert the data into QuestDB.

To open the QuestDB interface, open a browser and go to: http://localhost:9000/index.html

When finished remove all containers and volumes by running:
```
docker-compose down -v
```

# Mock gRPC Telemetric Data

To choose which mock data to use (healthy, thermal issues, mechanical issues), uncomment the dataframe with the data required, and comment the other two dataframes out in `telemetry_server.py`.

# Testing

There is a testing file attached to confirm the subscription works from the server to the client and the client is successfully entering the data in QuestDB. To test it run:
```
zsh test.sh
```

# SQL Queries

Below are some queries that can be ran in the QuestDB interface to get data.

###  Query to get entries in the last minute
```
SELECT *
FROM switch_telemetry
ORDER BY timestamp DESC
LIMIT 60;
```

###  Query to get start and end times of all downtimes in the last minute
```
WITH link_states AS (
    SELECT port, timestamp,
    CASE WHEN linkState = 'up' THEN 1 ELSE 0 END AS is_up
    FROM switch_telemetry
),
state_changes AS (
    SELECT port, timestamp, is_up,
    LAG(is_up) OVER (PARTITION BY port ORDER BY timestamp) AS prev_state
    FROM (
        SELECT DISTINCT port, timestamp, is_up
        FROM link_states
        ORDER BY port, timestamp
    )
),
flap_starts AS (
    SELECT port, timestamp AS flap_start_time
    FROM state_changes
    WHERE is_up = 0 AND (prev_state IS NULL OR prev_state = 1)
    AND timestamp >= (SELECT MAX(timestamp) - 60000000 FROM switch_telemetry)
)
SELECT
    f.port,
    f.flap_start_time AS down_time,
    MIN(u.timestamp) AS up_time,
    MIN(u.timestamp) - f.flap_start_time AS downtime_ms
FROM flap_starts f
JOIN link_states u ON f.port = u.port
    AND u.timestamp > f.flap_start_time
    AND u.is_up = 1
GROUP BY f.port, f.flap_start_time
ORDER BY f.port, f.flap_start_time;
```
  

###  Query to get data from 10 seconds before every downtime in the last minute
```
WITH link_states AS (
SELECT port, timestamp, switch_id, CASE WHEN linkState = 'up' THEN 1 ELSE 0 END AS is_up
FROM switch_telemetry
),
state_changes AS (
SELECT port, timestamp, switch_id, is_up, LAG(is_up) OVER (PARTITION BY switch_id, port ORDER BY timestamp) AS prev_state
FROM (
SELECT DISTINCT port, timestamp, switch_id, is_up FROM link_states ORDER BY switch_id, port, timestamp
)
),
flap_starts AS (
SELECT port, switch_id, timestamp AS flap_start_time
FROM state_changes
WHERE is_up = 0 AND (prev_state IS NULL OR prev_state = 1)
AND timestamp >= (SELECT MAX(timestamp) - 60000000 FROM switch_telemetry)
),
downtime_periods AS (
SELECT
f.port,
f.switch_id,
f.flap_start_time AS down_time,
MIN(u.timestamp) AS up_time,
EXTRACT(EPOCH FROM (MIN(u.timestamp) - f.flap_start_time)) * 1000 AS downtime_ms
FROM flap_starts f
JOIN link_states u ON f.port = u.port AND f.switch_id = u.switch_id
AND u.timestamp > f.flap_start_time AND u.is_up = 1
GROUP BY f.port, f.switch_id, f.flap_start_time
)
SELECT
t.timestamp,
t.switch_id,
t.port,
t.linkState,
t.SNR,
t.FEC_Correctable,
t.FEC_Uncorrectable,
t.CRCErrorCount,
t.Temperature,
t.Voltage,
t.FanSpeed,
t.Humidity,
t.Airflow,
t.AmbientTemperature,
t.OpticalRxPower,
t.OpticalTxPower,
t.LinkLatency,
t.CableLengthEstimate,
t.ConnectorInsertionCount,
d.down_time,
d.up_time,
d.downtime_ms
FROM switch_telemetry t
JOIN downtime_periods d ON t.port = d.port AND t.switch_id = d.switch_id
WHERE EXTRACT(EPOCH FROM (d.down_time - t.timestamp)) * 1000 <= 10000 -- 10 seconds in milliseconds
AND t.timestamp < d.down_time
ORDER BY t.switch_id, t.port, d.down_time, t.timestamp;
```