import grpc
import time
import telemetry_pb2
import telemetry_pb2_grpc
import psycopg2
from datetime import datetime

# Define thresholds for alerting
TEMP_THRESHOLD = 70.0  # °C
FAN_SPEED_MIN = 1000   # RPM

def create_questdb_table():
    """Create the telemetry table in QuestDB if it doesn't exist."""
    conn = psycopg2.connect(
        host="questdb",
        port=8812,
        user="admin",
        password="quest",
        database="qdb"
    )
    
    cursor = conn.cursor()
    
    # First drop the existing table if it exists
    try:
        cursor.execute("DROP TABLE IF EXISTS switch_telemetry;")
        conn.commit()
        print("Dropped existing table")
    except Exception as e:
        print(f"Error dropping table: {e}")
        conn.rollback()
    
    # Create a new partitioned table with proper settings for time-series data
    try:
        cursor.execute("""
        CREATE TABLE switch_telemetry (
            timestamp TIMESTAMP,
            switch_id SYMBOL,
            port SYMBOL,
            linkState SYMBOL,
            SNR DOUBLE,
            FEC_Correctable DOUBLE,
            FEC_Uncorrectable DOUBLE,
            CRCErrorCount DOUBLE,
            Temperature DOUBLE,
            Voltage DOUBLE,
            FanSpeed DOUBLE,
            Humidity DOUBLE,
            Airflow DOUBLE,
            AmbientTemperature DOUBLE,
            OpticalRxPower DOUBLE,
            OpticalTxPower DOUBLE,
            LinkLatency DOUBLE,
            CableLengthEstimate DOUBLE,
            ConnectorInsertionCount INT
        ) TIMESTAMP(timestamp) PARTITION BY DAY;
        """)
        conn.commit()
        print("QuestDB table created successfully with partitioning")
    except Exception as e:
        print(f"Error creating table: {e}")
        conn.rollback()
    
    cursor.close()
    conn.close()

def insert_telemetry(conn, telemetry, switch_id):
    """Insert telemetry data into QuestDB."""
    cursor = conn.cursor()
    
    # Parse timestamp to a proper datetime format if needed
    try:
        # Try to parse the timestamp string to a datetime object
        timestamp = datetime.strptime(telemetry.timestamp, "%Y-%m-%d %H:%M:%S")
        timestamp_iso = timestamp.isoformat()
    except ValueError:
        # If parsing fails, use the current time
        timestamp_iso = datetime.now().isoformat()
    
    # Prepare SQL insert statement
    sql = """
    INSERT INTO switch_telemetry (
        timestamp, switch_id, port, linkState, SNR, FEC_Correctable, 
        FEC_Uncorrectable, CRCErrorCount, Temperature, Voltage, 
        FanSpeed, Humidity, Airflow, AmbientTemperature, 
        OpticalRxPower, OpticalTxPower, LinkLatency, 
        CableLengthEstimate, ConnectorInsertionCount
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s, %s
    );
    """
    
    try:
        # Execute the SQL statement
        cursor.execute(sql, (
            timestamp_iso, switch_id, telemetry.port, telemetry.linkState,
            telemetry.SNR, telemetry.FEC_Correctable, telemetry.FEC_Uncorrectable,
            telemetry.CRCErrorCount, telemetry.Temperature, telemetry.Voltage,
            telemetry.FanSpeed, telemetry.Humidity, telemetry.Airflow,
            telemetry.AmbientTemperature, telemetry.OpticalRxPower, 
            telemetry.OpticalTxPower, telemetry.LinkLatency,
            telemetry.CableLengthEstimate, telemetry.ConnectorInsertionCount
        ))
        conn.commit()
        print(f"✅ Inserted data for port {telemetry.port} at {timestamp_iso}")
    except Exception as e:
        print(f"❌ Insert error: {e}")
        conn.rollback()
    
    cursor.close()


def run():
    # Create QuestDB table if it doesn't exist
    create_questdb_table()
    
    # Connect to QuestDB for data insertion
    questdb_conn = psycopg2.connect(
        host="questdb",  # Change from localhost to questdb
        port=8812,
        user="admin",
        password="quest",
        database="qdb"
    )

    # Add a delay to ensure the server is ready
    print("Waiting for telemetry server to start...")
    time.sleep(5)  # Give the server a moment to start
    
    # Connect to gRPC server
    with grpc.insecure_channel('telemetry-server:50051') as channel:  # Change from localhost to telemetry-server
        stub = telemetry_pb2_grpc.TelemetryServiceStub(channel)
        
        # Subscribe to telemetry data
        print("Subscribing to switch telemetry data...")
        switch_id = "switch1"
        request = telemetry_pb2.TelemetryRequest(switch_id=switch_id, port="")
        
        try:
            # Process streaming telemetry data
            for telemetry in stub.SubscribeTelemetry(request):
                # Print the basic telemetry data
                print(f"\nTimestamp: {telemetry.timestamp}")
                print(f"Port: {telemetry.port} | Link: {telemetry.linkState}")
                print(f"Temperature: {telemetry.Temperature:.1f}°C | Ambient: {telemetry.AmbientTemperature:.1f}°C")
                print(f"Fan Speed: {telemetry.FanSpeed:.0f} RPM | Humidity: {telemetry.Humidity:.1f}%")
                
                # Check for critical conditions
                alerts = []
                if telemetry.Temperature > TEMP_THRESHOLD:
                    alerts.append(f"⚠️ HIGH TEMPERATURE ALERT: {telemetry.Temperature:.1f}°C")
                if telemetry.FanSpeed < FAN_SPEED_MIN and telemetry.FanSpeed > 0:
                    alerts.append(f"⚠️ LOW FAN SPEED ALERT: {telemetry.FanSpeed:.0f} RPM")
                if telemetry.linkState == "down":
                    alerts.append(f"⚠️ LINK DOWN ALERT: Port {telemetry.port}")
                
                # Display any alerts
                for alert in alerts:
                    print(alert)
                
                # Insert the telemetry data into QuestDB
                try:
                    insert_telemetry(questdb_conn, telemetry, switch_id)
                    print(f"✅ Data inserted into QuestDB")
                except Exception as db_error:
                    print(f"❌ Database error: {db_error}")
                    
        except KeyboardInterrupt:
            print("Subscription canceled by user")
        except Exception as e:
            print(f"Error: {e}")
        finally:
            # Close QuestDB connection
            questdb_conn.close()

if __name__ == '__main__':
    run()