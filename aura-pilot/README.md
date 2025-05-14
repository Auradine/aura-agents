# Getting Started
To get started, open three terminal windows: one for the server, one for the client, and one for QuestDB. Then in client and server terminals, create and activate a virtual environment:
```
python3 -m venv venv
source venv/bin/activate
```
Then in these two terminals, install the required packages in the virtual environment
```
pip install grpcio grpcio-tools
pip install questdb psycopg2-binary
```
Then, we have to start everything. In the QuestDB terminal, run the following (make sure Docker is installed first):
```
docker run -p 9000:9000 -p 9009:9009 -p 8812:8812 questdb/questdb
```
In the server terminal run:
```
cd server
python telemetry_server.py
```
In the client terminal run:
```
cd client
python telemetry_client.py
```

To open the QuestDB interface, open a browser and go to: http://localhost:9000/index.html

# SQL Queries
Below are some queries that can be ran in the QuestDB interface to get data.

###  Query to get start and end times of all downtimes
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
  

###  Query to get data from 10 seconds before every downtime
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