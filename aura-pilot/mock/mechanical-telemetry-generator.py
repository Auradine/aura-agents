import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def generate_network_telemetry_with_mechanical_issues(minutes=10, interval_seconds=1):
    """
    Generate network telemetry time series data with link flapping due to mechanical stress or dust.
    
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
        'Temperature': {'base': 35.0, 'variation': 2.0},  # °C - normal temperature
        'Voltage': {'base': 3.3, 'variation': 0.1},  # V
        'FanSpeed': {'base': 3000, 'variation': 200},  # RPM
        'Humidity': {'base': 45.0, 'variation': 5.0},  # percentage
        'Airflow': {'base': 15.0, 'variation': 2.0},  # CFM
        'AmbientTemperature': {'base': 22.0, 'variation': 1.0},  # °C
        'OpticalRxPower': {'base': -5.0, 'variation': 1.0},  # dBm - will degrade with dust/stress
        'OpticalTxPower': {'base': 0.0, 'variation': 0.5},  # dBm - will degrade with dust/stress
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
    
    # Create optical power degradation profile (mimicking dust buildup or cable stress)
    # This will affect both Rx and Tx power, as well as link stability
    
    # Divide timeline into sections
    normal_period = int(total_points * 0.2)  # First 20% is normal
    early_issue_period = int(total_points * 0.2)  # Next 20% shows early signs of issues
    moderate_issue_period = int(total_points * 0.3)  # Next 30% shows moderate issues
    severe_issue_period = total_points - normal_period - early_issue_period - moderate_issue_period  # Last 30% has severe issues
    
    # Create optical power degradation pattern
    optical_degradation = []
    
    # Normal period - stable optical power
    for i in range(normal_period):
        optical_degradation.append(0.0)  # No degradation
    
    # Early issue period - minor degradation (dust building up or cable stress beginning)
    for i in range(early_issue_period):
        progress = i / early_issue_period
        degradation = progress * 1.0  # Up to 1.0 dB degradation
        optical_degradation.append(degradation)
    
    # Moderate issue period - increased degradation with minor fluctuations (dust or slight movements)
    start_degradation = optical_degradation[-1]
    for i in range(moderate_issue_period):
        progress = i / moderate_issue_period
        degradation = start_degradation + progress * 2.0  # Additional 2.0 dB degradation
        # Add small fluctuations
        degradation += np.random.normal(0, 0.3)
        optical_degradation.append(degradation)
    
    # Severe issue period - significant degradation with major fluctuations (loose connection)
    start_degradation = optical_degradation[-1]
    for i in range(severe_issue_period):
        # Base degradation
        degradation = start_degradation + 2.0  # Additional 2.0 dB degradation
        
        # Add major fluctuations to simulate physical movement or vibration effects
        # Every ~5-10 seconds, add a significant jitter
        if i % random.randint(5, 10) == 0:
            degradation += np.random.choice([-2.0, -1.5, -1.0, -0.5, 0.5, 1.0, 1.5, 2.0])
        else:
            degradation += np.random.normal(0, 0.5)
            
        optical_degradation.append(degradation)
    
    # Generate data for the port
    last_link_state = "up"  # Start with link up
    link_flap_cooldown = 0  # Cooldown counter to prevent too rapid flapping
    
    for i, ts in enumerate(timestamps):
        # Get current optical degradation
        current_degradation = optical_degradation[i]
        
        # Determine if link flaps based on optical degradation level
        if current_degradation < 1.0:
            # Minor degradation, very stable link
            flap_probability = 0.001  # 0.1% chance of flapping
        elif current_degradation < 2.0:
            # Moderate degradation, occasional flaps
            flap_probability = 0.02  # 2% chance of flapping
        elif current_degradation < 3.0:
            # Significant degradation, more frequent flaps
            flap_probability = 0.08  # 8% chance of flapping
        else:
            # Severe degradation, highly unstable
            flap_probability = 0.25  # 25% chance of flapping
        
        # Link state logic with cooldown to prevent unrealistically rapid flapping
        if link_flap_cooldown > 0:
            link_flap_cooldown -= 1
            link_state = last_link_state
        else:
            if random.random() < flap_probability:
                # Flip the link state
                link_state = "down" if last_link_state == "up" else "up"
                
                # When a severe flap occurs, simulate a physical jiggle of the cable
                # that might temporarily improve or worsen the connection
                if current_degradation > 2.0 and link_state == "up":
                    # A brief improvement (cable was jiggled to better position)
                    link_flap_cooldown = random.randint(5, 15)  # 5-15 seconds of stability
                else:
                    # Normal cooldown
                    link_flap_cooldown = random.randint(3, 8)  # 3-8 seconds of stability
                    
                last_link_state = link_state
            else:
                link_state = last_link_state
        
        # Create row with consistent column order
        row = {
            'timestamp': ts,
            'port': 'port1',
            'linkState': link_state
        }
        
        # SNR degrades with optical degradation
        snr_impact = current_degradation * 2  # 1dB optical loss = ~2dB SNR reduction
        if link_state == "up":
            row['SNR'] = round(healthy_params['SNR']['base'] - snr_impact + np.random.normal(0, healthy_params['SNR']['variation']), 3)
            # Add more jitter to SNR during severe issues
            if current_degradation > 3.0:
                row['SNR'] += np.random.normal(0, 2.0)
        else:
            row['SNR'] = None
        
        # Error counters based on optical degradation
        error_factor = 1 + max(0, current_degradation)
        
        # FEC Correctable errors (increase with degradation)
        if link_state == "up":
            if random.random() < 0.1 * error_factor:
                new_correctable = max(0, int(np.random.normal(healthy_params['FEC_Correctable']['base']/10, 
                                                           healthy_params['FEC_Correctable']['variation']/5) * error_factor))
                cumulative_errors['FEC_Correctable'] += new_correctable
            row['FEC_Correctable'] = cumulative_errors['FEC_Correctable']
            
            # FEC Uncorrectable errors (only appear with severe degradation)
            if current_degradation > 3.0 and random.random() < 0.08 * error_factor:
                new_uncorrectable = max(0, int(np.random.normal(1, 1)))
                cumulative_errors['FEC_Uncorrectable'] += new_uncorrectable
            row['FEC_Uncorrectable'] = cumulative_errors['FEC_Uncorrectable']
        else:
            row['FEC_Correctable'] = None
            row['FEC_Uncorrectable'] = None
        
        # CRC errors (increase with degradation)
        if link_state == "up":
            if random.random() < 0.05 * error_factor:
                new_crc = max(0, int(np.random.normal(1, 1) * error_factor))
                cumulative_errors['CRCErrorCount'] += new_crc
            row['CRCErrorCount'] = cumulative_errors['CRCErrorCount']
        else:
            row['CRCErrorCount'] = None
        
        # Temperature (normal and stable)
        row['Temperature'] = round(healthy_params['Temperature']['base'] + np.random.normal(0, healthy_params['Temperature']['variation']), 3)
        
        # Voltage (normal with slight fluctuations during severe issues)
        if link_state == "up":
            voltage_variation = healthy_params['Voltage']['variation']
            if current_degradation > 3.0:
                voltage_variation = voltage_variation * 1.5  # More fluctuation during severe issues
            row['Voltage'] = round(healthy_params['Voltage']['base'] + np.random.normal(0, voltage_variation), 3)
        else:
            row['Voltage'] = None
        
        # Environmental factors (normal and stable)
        row['FanSpeed'] = round(healthy_params['FanSpeed']['base'] + np.random.normal(0, healthy_params['FanSpeed']['variation']), 3)
        row['Humidity'] = round(healthy_params['Humidity']['base'] + np.random.normal(0, healthy_params['Humidity']['variation']), 3)
        row['Airflow'] = round(healthy_params['Airflow']['base'] + np.random.normal(0, healthy_params['Airflow']['variation']), 3)
        row['AmbientTemperature'] = round(healthy_params['AmbientTemperature']['base'] + np.random.normal(0, healthy_params['AmbientTemperature']['variation']), 3)
        
        # Optical power degradation (key indicator of dust/mechanical issues)
        if link_state == "up":
            # More significant impact on RX than TX
            rx_loss = current_degradation * 1.2  # Dust/bending affects receiving more than transmitting
            tx_loss = current_degradation * 0.7
            
            # Add jitter to optical power during severe issues
            rx_jitter = np.random.normal(0, 0.2) * min(1, current_degradation/2)
            tx_jitter = np.random.normal(0, 0.1) * min(1, current_degradation/2)
            
            row['OpticalRxPower'] = round(healthy_params['OpticalRxPower']['base'] - rx_loss - rx_jitter + np.random.normal(0, healthy_params['OpticalRxPower']['variation']), 3)
            row['OpticalTxPower'] = round(healthy_params['OpticalTxPower']['base'] - tx_loss - tx_jitter + np.random.normal(0, healthy_params['OpticalTxPower']['variation']), 3)
        else:
            row['OpticalRxPower'] = None
            row['OpticalTxPower'] = None
        
        # Link latency (increases with degradation)
        if link_state == "up":
            latency_increase = current_degradation * 5  # 5µs increase per 1dB degradation
            row['LinkLatency'] = round(healthy_params['LinkLatency']['base'] + latency_increase + np.random.normal(0, healthy_params['LinkLatency']['variation']), 3)
            
            # Add jitter to latency during severe issues
            if current_degradation > 3.0:
                row['LinkLatency'] += abs(np.random.normal(0, 10))
        else:
            row['LinkLatency'] = None
        
        # Cable length (stable with slight variations in severe cases)
        # Mechanical stress can cause fluctuations in estimated cable length
        if current_degradation > 3.0 and link_state == "up":
            # Simulate cable length fluctuations due to severe mechanical stress
            row['CableLengthEstimate'] = round(healthy_params['CableLengthEstimate']['base'] + np.random.normal(0, 0.5), 3)
        else:
            row['CableLengthEstimate'] = healthy_params['CableLengthEstimate']['base']
        
        # Connector insertion count (increments when cable is reseated)
        # Simulate a cable reseating at one point during severe issues
        if i == int(total_points * 0.8) and link_state == "down":  # At 80% mark, simulate cable reseating
            cumulative_errors['ConnectorInsertionCount'] += 1
            # Reset degradation temporarily after cable reseating
            for j in range(i, min(i+30, len(optical_degradation))):
                optical_degradation[j] = max(0, optical_degradation[j] - 2.0)
        
        row['ConnectorInsertionCount'] = cumulative_errors['ConnectorInsertionCount']
        
        data.append(row)
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Ensure timestamp column is datetime type
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    return df

def save_to_csv(df, filename="mechanicaltelemetry.csv"):
    """Save telemetry data to CSV file."""
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")
    print(f"Generated {len(df)} data points")
    
    # Print link flap statistics
    up_count = df[df['linkState'] == 'up'].shape[0]
    down_count = df[df['linkState'] == 'down'].shape[0]
    print(f"Link state statistics: {up_count} up ({up_count/len(df)*100:.1f}%), {down_count} down ({down_count/len(df)*100:.1f}%)")
    
    # Print optical power range
    print(f"Optical RX power range: {df['OpticalRxPower'].min():.1f} dBm to {df['OpticalRxPower'].max():.1f} dBm")

if __name__ == "__main__":
    # Generate 10 minutes of telemetry data with 1-second intervals showing mechanical/dust issues
    telemetry_data = generate_network_telemetry_with_mechanical_issues(minutes=10, interval_seconds=1)
    
    # Save to CSV
    save_to_csv(telemetry_data)
    
    # Display sample data
    print("\nSample of the generated data:")
    print(telemetry_data.head())
    
    # Display summary statistics
    print("\nSummary statistics for numeric columns:")
    print(telemetry_data.describe())