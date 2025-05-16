import grpc
import os
import time
import datetime
from concurrent import futures
import telemetry_pb2
import telemetry_pb2_grpc
import threading
import random
import pandas as pd
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from mock_telemetry_generator import thermal_telemetry_generator
from mock_telemetry_generator import mechanical_telemetry_generator
from mock_telemetry_generator import healthy_telemetry_generator


class TelemetryServicer(telemetry_pb2_grpc.TelemetryServiceServicer):
    def SubscribeTelemetry(self, request, context):
        # Generate a 10-minute window of data, then loop
        interval_seconds = 1
        minutes = 2
        while True:
            # choose one of the following, and comment out the others:
            df = thermal_telemetry_generator(minutes=minutes, interval_seconds=interval_seconds)
            # df = mechanical_telemetry_generator(minutes=minutes, interval_seconds=interval_seconds)
            # df = healthy_telemetry_generator(minutes=minutes, interval_seconds=interval_seconds)
            
            filtered_data = df
            if request.port:
                filtered_data = df[df['port'] == request.port]
            for _, row in filtered_data.iterrows():
                telemetry_entry = telemetry_pb2.TelemetryData(
                    timestamp=str(row['timestamp']),
                    port=row['port'],
                    linkState=row['linkState'],
                    SNR=float(row['SNR']) if not pd.isnull(row['SNR']) else 0.0,
                    FEC_Correctable=float(row['FEC_Correctable']) if not pd.isnull(row['FEC_Correctable']) else 0.0,
                    FEC_Uncorrectable=float(row['FEC_Uncorrectable']) if not pd.isnull(row['FEC_Uncorrectable']) else 0.0,
                    CRCErrorCount=float(row['CRCErrorCount']) if not pd.isnull(row['CRCErrorCount']) else 0.0,
                    Temperature=float(row['Temperature']) if not pd.isnull(row['Temperature']) else 0.0,
                    Voltage=float(row['Voltage']) if not pd.isnull(row['Voltage']) else 0.0,
                    FanSpeed=float(row['FanSpeed']) if not pd.isnull(row['FanSpeed']) else 0.0,
                    Humidity=float(row['Humidity']) if not pd.isnull(row['Humidity']) else 0.0,
                    Airflow=float(row['Airflow']) if not pd.isnull(row['Airflow']) else 0.0,
                    AmbientTemperature=float(row['AmbientTemperature']) if not pd.isnull(row['AmbientTemperature']) else 0.0,
                    OpticalRxPower=float(row['OpticalRxPower']) if not pd.isnull(row['OpticalRxPower']) else 0.0,
                    OpticalTxPower=float(row['OpticalTxPower']) if not pd.isnull(row['OpticalTxPower']) else 0.0,
                    LinkLatency=float(row['LinkLatency']) if not pd.isnull(row['LinkLatency']) else 0.0,
                    CableLengthEstimate=float(row['CableLengthEstimate']) if not pd.isnull(row['CableLengthEstimate']) else 0.0,
                    ConnectorInsertionCount=int(row['ConnectorInsertionCount']) if not pd.isnull(row['ConnectorInsertionCount']) else 0
                )
                yield telemetry_entry
                time.sleep(interval_seconds)
            # After 10 minutes, loop again


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    telemetry_pb2_grpc.add_TelemetryServiceServicer_to_server(TelemetryServicer(), server)
    server.add_insecure_port('0.0.0.0:50051')  # Change from [::]:50051 to 0.0.0.0:50051
    server.start()
    print("Telemetry Server started, listening on port 50051")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()