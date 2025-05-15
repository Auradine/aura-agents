import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# --- Healthy ---
def healthy_telemetry_generator(port_count=1, minutes=10, interval_seconds=1):
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

# --- Thermal ---
def thermal_telemetry_generator(minutes=10, interval_seconds=1):
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

# --- Mechanical ---
def mechanical_telemetry_generator(minutes=10, interval_seconds=1):
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
