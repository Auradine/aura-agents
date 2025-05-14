import grpc
import csv
import time
import datetime
from concurrent import futures
import telemetry_pb2
import telemetry_pb2_grpc

class TelemetryServicer(telemetry_pb2_grpc.TelemetryServiceServicer):
    def SubscribeTelemetry(self, request, context):
        # Load the telemetry data from CSV
        telemetry_data = []
        with open('thermaltelemetry.csv', 'r') as csvfile: #change csv here
            reader = csv.DictReader(csvfile)
            for row in reader:
                telemetry_data.append(row)
        
        print(f"Client subscribed to switch telemetry for port: {request.port}")
        
        filtered_data = telemetry_data[:]
        if request.port:
            filtered_data = [row for row in filtered_data if row['port'] == request.port]
        
        # Stream the data
        for row in filtered_data:
            try:
                # Safely convert values with proper handling for empty strings
                telemetry_entry = telemetry_pb2.TelemetryData(
                    timestamp=row['timestamp'],
                    port=row['port'],
                    linkState=row['linkState'],
                    SNR=float(row['SNR']) if row['SNR'] else 0.0,
                    FEC_Correctable=float(row['FEC_Correctable']) if row['FEC_Correctable'] else 0.0,
                    FEC_Uncorrectable=float(row['FEC_Uncorrectable']) if row['FEC_Uncorrectable'] else 0.0,
                    CRCErrorCount=float(row['CRCErrorCount']) if row['CRCErrorCount'] else 0.0,
                    Temperature=float(row['Temperature']) if row['Temperature'] else 0.0,
                    Voltage=float(row['Voltage']) if row['Voltage'] else 0.0,
                    FanSpeed=float(row['FanSpeed']) if row['FanSpeed'] else 0.0,
                    Humidity=float(row['Humidity']) if row['Humidity'] else 0.0,
                    Airflow=float(row['Airflow']) if row['Airflow'] else 0.0,
                    AmbientTemperature=float(row['AmbientTemperature']) if row['AmbientTemperature'] else 0.0,
                    OpticalRxPower=float(row['OpticalRxPower']) if row['OpticalRxPower'] else 0.0,
                    OpticalTxPower=float(row['OpticalTxPower']) if row['OpticalTxPower'] else 0.0,
                    LinkLatency=float(row['LinkLatency']) if row['LinkLatency'] else 0.0,
                    CableLengthEstimate=float(row['CableLengthEstimate']) if row['CableLengthEstimate'] else 0.0,
                    ConnectorInsertionCount=int(row['ConnectorInsertionCount']) if row['ConnectorInsertionCount'] else 0
                )
                
                # Yield the data to the client
                yield telemetry_entry
                
            except Exception as e:
                print(f"Error processing row: {e}")
                print(f"Problematic row data: {row}")
                # Continue with next row instead of crashing
                continue
                
            # Wait 1 second before sending the next entry
            # time.sleep(1)
        
        print("Finished streaming telemetry data")


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    telemetry_pb2_grpc.add_TelemetryServiceServicer_to_server(TelemetryServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Telemetry Server started, listening on port 50051")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()