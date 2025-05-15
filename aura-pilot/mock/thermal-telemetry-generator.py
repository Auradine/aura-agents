import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def generate_network_telemetry_with_flapping(minutes=10, interval_seconds=1):
    """
    Generate network telemetry time series data with link flapping due to temperature increases.
    
    Args:
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
    
    # Healthy network baseline parameters
    healthy_params = {
        'SNR': {'base': 32.0, 'variation': 3.0},  # dB (good SNR > 25dB)
        'FEC_Correctable': {'base': 10, 'variation': 5},  # Low error count
        'FEC_Uncorrectable': {'base': 0, 'variation': 1},  # Very rare errors
        'CRCErrorCount': {'base': 0, 'variation': 2},  # Very rare errors
        'Temperature': {'base': 35.0, 'variation': 2.0},  # °C - will increase over time
        'Voltage': {'base': 3.3, 'variation': 0.1},  # V
        'FanSpeed': {'base': 3000, 'variation': 200},  # RPM
        'Humidity': {'base': 45.0, 'variation': 5.0},  # percentage
        'Airflow': {'base': 15.0, 'variation': 2.0},  # CFM
        'AmbientTemperature': {'base': 22.0, 'variation': 1.0},  # °C
        'OpticalRxPower': {'base': -5.0, 'variation': 1.0},  # dBm
        'OpticalTxPower': {'base': 0.0, 'variation': 0.5},  # dBm
        'LinkLatency': {'base': 50.0, 'variation': 10.0},  # µs
        'CableLengthEstimate': {'base': 10.0, 'variation': 0.0},  # m (constant)
        'ConnectorInsertionCount': {'base': 5, 'variation': 0}  # integer (constant)
    }
    
    # Initialize cumulative error counters
    cumulative_errors = {
        'FEC_Correctable': 0,
        'FEC_Uncorrectable': 0,
        'CRCErrorCount': 0,
        'ConnectorInsertionCount': int(healthy_params['ConnectorInsertionCount']['base'])
    }
    
    # Temperature progression pattern
    # We'll model a temperature that starts normal and then rises beyond acceptable thresholds
    # This will cause link flapping in the second half of the time series
    temp_progression = []
    
    # Start with normal temperature
    normal_period = int(total_points * 0.3)  # First 30% is normal
    rising_period = int(total_points * 0.3)  # Next 30% shows rising temperatures
    high_period = total_points - normal_period - rising_period  # Last 40% has high temperatures with flapping
    
    # Normal temperature period
    for i in range(normal_period):
        base_temp = healthy_params['Temperature']['base']
        variation = healthy_params['Temperature']['variation']
        temp = base_temp + np.random.normal(0, variation * 0.5)
        temp_progression.append(temp)
    
    # Rising temperature period
    start_temp = temp_progression[-1]
    target_temp = 55.0  # Rising to concerning level (55°C)
    for i in range(rising_period):
        progress = i / rising_period
        temp = start_temp + (target_temp - start_temp) * progress
        # Add some noise
        temp += np.random.normal(0, 1.0)
        temp_progression.append(temp)
    
    # High temperature period with fluctuations
    for i in range(high_period):
        # Fluctuating around high temperature
        temp = target_temp + np.random.normal(0, 2.0)
        temp_progression.append(temp)
    
    # Generate data for the port
    last_link_state = "up"  # Start with link up
    link_flap_cooldown = 0  # Cooldown counter to prevent too rapid flapping
    
    for i, ts in enumerate(timestamps):
        # Get current temperature
        current_temp = temp_progression[i]
        
        # Determine if link flaps based on temperature
        # Higher temperature increases flap probability
        if current_temp < 45:
            # Normal temperature, stable link (near 100% uptime)
            flap_probability = 0.001  # 0.1% chance of flapping at normal temp
        elif current_temp < 50:
            # Elevated temperature, occasional flaps
            flap_probability = 0.05  # 5% chance of flapping
        else:
            # High temperature, frequent flaps
            flap_probability = 0.20  # 20% chance of flapping 
        
        # Link state logic with cooldown to prevent unrealistically rapid flapping
        if link_flap_cooldown > 0:
            link_flap_cooldown -= 1
            link_state = last_link_state
        else:
            if random.random() < flap_probability:
                # Flip the link state
                link_state = "down" if last_link_state == "up" else "up"
                # Set a cooldown period to prevent flapping every second
                link_flap_cooldown = random.randint(3, 10)  # 3-10 seconds of stability
                last_link_state = link_state
            else:
                link_state = last_link_state
        
        # Create row with same column order as original script
        row = {
            'timestamp': ts,
            'port': 'port1',
            'linkState': link_state
        }
        
        # Add SNR (affected by temperature)
        if link_state == "up":
            temp_factor = max(0, 1 - (current_temp - 35) / 30)  # Normalize temperature effect
            snr_base = healthy_params['SNR']['base'] * temp_factor
            row['SNR'] = round(snr_base + np.random.normal(0, healthy_params['SNR']['variation']), 3)
        else:
            row['SNR'] = None
            
        # Add FEC errors (increase with temperature)
        error_factor = 1 + max(0, (current_temp - 40) / 10)  # Error multiplier based on temperature
        
        if link_state == "up":
            # FEC Correctable errors
            if random.random() < 0.1 * error_factor:  # Increased chance of errors at high temp
                new_correctable = max(0, int(np.random.normal(healthy_params['FEC_Correctable']['base']/20, 
                                                           healthy_params['FEC_Correctable']['variation']/10) * error_factor))
                cumulative_errors['FEC_Correctable'] += new_correctable
            row['FEC_Correctable'] = cumulative_errors['FEC_Correctable']
            
            # FEC Uncorrectable errors
            if current_temp > 48 and random.random() < 0.05 * error_factor:
                new_uncorrectable = max(0, int(np.random.normal(1, 1)))
                cumulative_errors['FEC_Uncorrectable'] += new_uncorrectable
            row['FEC_Uncorrectable'] = cumulative_errors['FEC_Uncorrectable']
        else:
            row['FEC_Correctable'] = None
            row['FEC_Uncorrectable'] = None
            
        # Add CRC errors
        if link_state == "up":
            if random.random() < 0.05 * error_factor:
                new_crc = max(0, int(np.random.normal(1, 1) * error_factor))
                cumulative_errors['CRCErrorCount'] += new_crc
            row['CRCErrorCount'] = cumulative_errors['CRCErrorCount']
        else:
            row['CRCErrorCount'] = None
            
        # Always include temperature (even when link is down)
        row['Temperature'] = round(current_temp, 3)
        
        # Add voltage
        if link_state == "up":
            row['Voltage'] = round(healthy_params['Voltage']['base'] + np.random.normal(0, healthy_params['Voltage']['variation']), 3)
        else:
            row['Voltage'] = None
            
        # Fan speed increases with temperature (available even when link is down)
        fan_adjustment = max(0, (current_temp - 40) * 50)  # Fan speeds up as temp rises above 40°C
        row['FanSpeed'] = round(healthy_params['FanSpeed']['base'] + fan_adjustment + np.random.normal(0, healthy_params['FanSpeed']['variation']), 3)
        
        # Environmental metrics (available even when link is down)
        row['Humidity'] = round(healthy_params['Humidity']['base'] + np.random.normal(0, healthy_params['Humidity']['variation']), 3)
        row['Airflow'] = round(healthy_params['Airflow']['base'] + np.random.normal(0, healthy_params['Airflow']['variation']), 3)
        row['AmbientTemperature'] = round(healthy_params['AmbientTemperature']['base'] + np.random.normal(0, healthy_params['AmbientTemperature']['variation']), 3)
        
        # Optical power metrics
        if link_state == "up":
            rx_degradation = max(0, (current_temp - 45) * 0.1)  # More degradation at high temps
            row['OpticalRxPower'] = round(healthy_params['OpticalRxPower']['base'] - rx_degradation + np.random.normal(0, healthy_params['OpticalRxPower']['variation']), 3)
            row['OpticalTxPower'] = round(healthy_params['OpticalTxPower']['base'] - rx_degradation/2 + np.random.normal(0, healthy_params['OpticalTxPower']['variation']), 3)
        else:
            row['OpticalRxPower'] = None
            row['OpticalTxPower'] = None
            
        # Link latency
        if link_state == "up":
            latency_increase = max(0, (current_temp - 40) * 2)  # Latency increases as temp rises above 40°C
            row['LinkLatency'] = round(healthy_params['LinkLatency']['base'] + latency_increase + np.random.normal(0, healthy_params['LinkLatency']['variation']), 3)
        else:
            row['LinkLatency'] = None
            
        # Cable length and connector insertion count (static values)
        row['CableLengthEstimate'] = healthy_params['CableLengthEstimate']['base']
        row['ConnectorInsertionCount'] = cumulative_errors['ConnectorInsertionCount']
        
        data.append(row)
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Ensure timestamp column is datetime type
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    return df

def save_to_csv(df, filename="thermaltelemetry.csv"):
    """Save telemetry data to CSV file."""
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")
    print(f"Generated {len(df)} data points")
    
    # Print link flap statistics
    up_count = df[df['linkState'] == 'up'].shape[0]
    down_count = df[df['linkState'] == 'down'].shape[0]
    print(f"Link state statistics: {up_count} up ({up_count/len(df)*100:.1f}%), {down_count} down ({down_count/len(df)*100:.1f}%)")
    
    # Print temperature range
    print(f"Temperature range: {df['Temperature'].min():.1f}°C to {df['Temperature'].max():.1f}°C")

if __name__ == "__main__":
    # Generate 10 minutes of telemetry data with 1-second intervals showing temperature-induced link flapping
    telemetry_data = generate_network_telemetry_with_flapping(minutes=10, interval_seconds=1)
    
    # Save to CSV
    save_to_csv(telemetry_data)
    
    # Display sample data
    print("\nSample of the generated data:")
    print(telemetry_data.head())
    
    # Display summary statistics
    print("\nSummary statistics for numeric columns:")
    print(telemetry_data.describe())