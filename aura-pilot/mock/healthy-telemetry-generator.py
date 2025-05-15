import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def generate_network_telemetry(port_count=1, minutes=10, interval_seconds=1):
    """
    Generate healthy network telemetry time series data.
    
    Args:
        port_count: Number of ports to generate data for
        minutes: Number of minutes of data to generate
        interval_seconds: Interval between measurements in seconds
    
    Returns:
        DataFrame containing the telemetry data
    """
    # Calculate number of data points
    total_points = int((minutes * 60) / interval_seconds)
    
    # Generate timestamps
    end_time = datetime.now().replace(microsecond=0)
    start_time = end_time - timedelta(minutes=minutes)
    timestamps = [start_time + timedelta(seconds=i*interval_seconds) for i in range(total_points)]
    
    # Initialize data list
    data = []
    
    # Healthy network parameters (baseline values and normal variation ranges)
    healthy_params = {
        'SNR': {'base': 32.0, 'variation': 3.0},  # dB (good SNR > 25dB)
        'FEC_Correctable': {'base': 10, 'variation': 5},  # Low error count
        'FEC_Uncorrectable': {'base': 0, 'variation': 1},  # Very rare errors
        'CRCErrorCount': {'base': 0, 'variation': 2},  # Very rare errors
        'Temperature': {'base': 35.0, 'variation': 5.0},  # °C
        'Voltage': {'base': 3.3, 'variation': 0.1},  # V
        'FanSpeed': {'base': 3000, 'variation': 200},  # RPM
        'Humidity': {'base': 45.0, 'variation': 5.0},  # percentage
        'Airflow': {'base': 15.0, 'variation': 2.0},  # CFM
        'AmbientTemperature': {'base': 22.0, 'variation': 2.0},  # °C
        'OpticalRxPower': {'base': -5.0, 'variation': 1.0},  # dBm
        'OpticalTxPower': {'base': 0.0, 'variation': 0.5},  # dBm
        'LinkLatency': {'base': 50.0, 'variation': 10.0},  # µs
        'CableLengthEstimate': {'base': 10.0, 'variation': 0.0},  # m (constant)
        'ConnectorInsertionCount': {'base': 5, 'variation': 0}  # integer (constant)
    }
    
    # Generate data for each port
    for port in range(1, port_count + 1):
        # For each port, we want consistent but slightly different baseline values
        port_params = {k: {'base': v['base'] + (port - 2.5) * 0.1 * v['base'], 
                          'variation': v['variation']} 
                      for k, v in healthy_params.items()}
        
        # Link state (mostly up with rare downs)
        up_probability = 0.995  # 99.5% uptime for a healthy network
        
        # Initialize port-specific error accumulators (these accumulate over time)
        cumulative_errors = {
            'FEC_Correctable': 0,
            'FEC_Uncorrectable': 0,
            'CRCErrorCount': 0,
            'ConnectorInsertionCount': int(port_params['ConnectorInsertionCount']['base'])
        }
        
        for ts in timestamps:
            # Determine if link is up (with high probability for healthy network)
            link_state = "up" if random.random() < up_probability else "down"
            
            # If link is down, we'll add a row with the down state and skip to next timestamp
            if link_state == "down":
                row = {
                    'timestamp': ts,
                    'port': f'port{port}',
                    'linkState': 'down'
                }
                # Fill other metrics with None during downtime
                for metric in healthy_params.keys():
                    row[metric] = None
                data.append(row)
                continue
            
            # Generate values with small variations (normal distribution)
            row = {
                'timestamp': ts,
                'port': f'port{port}',
                'linkState': 'up'
            }
            
            # Add small daily variation pattern to some metrics
            hour_of_day = ts.hour + ts.minute/60
            daily_factor = np.sin(hour_of_day / 24 * 2 * np.pi)
            
            # Generate each metric
            for metric, params in port_params.items():
                base = params['base']
                variation = params['variation']
                
                if metric in ['FEC_Correctable', 'FEC_Uncorrectable', 'CRCErrorCount']:
                    # These are rare, incremental error counters
                    # For a healthy network, we want to see very few of these
                    if random.random() < 0.05:  # 5% chance of any error
                        new_errors = max(0, int(np.random.normal(params['base']/100, params['variation']/50)))
                        cumulative_errors[metric] += new_errors
                    row[metric] = cumulative_errors[metric]
                
                elif metric == 'ConnectorInsertionCount':
                    # This rarely changes - maybe once in the entire dataset
                    if random.random() < 0.0005:  # Very rare chance of a new insertion
                        cumulative_errors[metric] += 1
                    row[metric] = cumulative_errors[metric]
                
                elif metric == 'CableLengthEstimate':
                    # This is constant for a given port
                    row[metric] = base
                
                elif metric in ['Temperature', 'Voltage', 'FanSpeed']:
                    # These metrics might show a small daily pattern
                    daily_variation = daily_factor * variation * 0.5
                    random_variation = np.random.normal(0, variation * 0.5)
                    row[metric] = base + daily_variation + random_variation
                    
                elif metric == 'AmbientTemperature':
                    # Ambient temperature varies with time of day
                    daily_variation = daily_factor * 2.0  # 2°C variation through the day
                    random_variation = np.random.normal(0, 0.5)  # Small random fluctuations
                    row[metric] = base + daily_variation + random_variation
                
                elif metric == 'Humidity':
                    # Humidity often inversely correlates with temperature
                    daily_variation = -daily_factor * 3.0  # Inverse of temperature pattern
                    random_variation = np.random.normal(0, 1.0)
                    row[metric] = base + daily_variation + random_variation
                    # Ensure humidity stays in reasonable range
                    row[metric] = min(max(row[metric], 30), 70)
                
                else:
                    # Other metrics have simple random variations
                    row[metric] = base + np.random.normal(0, variation)
            
            # Round floating point values for clarity
            for key, value in row.items():
                if isinstance(value, float):
                    row[key] = round(value, 3)
            
            data.append(row)
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Ensure timestamp column is datetime type
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    return df

def save_to_csv(df, filename="healthytelemetry.csv"):
    """Save telemetry data to CSV file."""
    last_column = df.columns[-1]
    
    # Convert the last column to integer if it's numeric
    if pd.api.types.is_numeric_dtype(df[last_column]):
        df[last_column] = df[last_column].astype('Int64')
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")
    print(f"Generated {len(df)} data points for {df['port'].nunique()} ports")

if __name__ == "__main__":
    # Generate 10 minutes of telemetry data with 1-second intervals for 1 port
    telemetry_data = generate_network_telemetry(port_count=1, minutes=10, interval_seconds=1)
    
    # Save to CSV
    save_to_csv(telemetry_data)
    
    # Display sample data
    print("\nSample of the generated data:")
    print(telemetry_data.head())
    
    # Display summary statistics
    print("\nSummary statistics for numeric columns:")
    print(telemetry_data.describe())