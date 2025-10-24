import math
from datetime import datetime, timezone

def calculate_heading(lat1, lon1, lat2, lon2):
    delta_lon = math.radians(lon2 - lon1)
    y = math.sin(delta_lon) * math.cos(math.radians(lat2))
    x = math.cos(math.radians(lat1)) * math.sin(math.radians(lat2)) - \
        math.sin(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.cos(delta_lon)
    return (math.degrees(math.atan2(y, x)) + 360) % 360

def vector_magnitude(vector):
    return math.sqrt(sum(component ** 2 for component in vector))

# Parse session data
def parse_session_data(session_str):
    raw_points = session_str.strip().split("|")
    data = []
    for point in raw_points:
        parts = point.split(",")
        if len(parts) != 11:
            continue
        lat, lon = float(parts[0]), float(parts[1])
        velocity = float(parts[3])
        timestamp = int(parts[4])
        gyro = tuple(map(float, parts[5:8]))
        accel = tuple(map(float, parts[8:11]))
        data.append({
            "lat": lat,
            "lon": lon,
            "velocity": velocity,
            "timestamp": timestamp,
            "gyro": gyro,
            "accel": accel
        })
    return data

# Detect cornering events
def detect_cornering_events(data, config):
    events = []
    last_event_end_time = 0 # track end of last cornering event
    
    # Get config values with defaults
    general_turn_threshold = config.get('GENERAL_TURN_THRESHOLD_DEG_S', 15)
    hard_turn_threshold = config.get('HARD_TURN_THRESHOLD_DEG_S', 75)
    general_lateral_accel = config.get('GENERAL_LATERAL_ACCEL_G', 0.1)
    hard_lateral_accel = config.get('HARD_LATERAL_ACCEL_G', 0.4)
    turning_time_limit = config.get('TURNING_TIME_LIMIT', 15)
    cooldown_period = config.get('COOLDOWN_PERIOD', 3)
    
    for i in range(1, len(data) - 1):
        prev_point = data[i - 1]
        current_point = data[i]
        next_point = data[i + 1]

        time_delta = next_point["timestamp"] - prev_point["timestamp"]
        if time_delta == 0:
            continue

        # Angular velocity magnitude (rad/s → deg/s)
        angular_velocity_magnitude_rad = vector_magnitude(current_point["gyro"])
        angular_velocity_magnitude_deg = math.degrees(angular_velocity_magnitude_rad)

        # Lateral acceleration magnitude (in g)
        lateral_acceleration_magnitude = vector_magnitude(current_point["accel"])

        # Heading change using GPS
        heading_before = calculate_heading(prev_point["lat"], prev_point["lon"], current_point["lat"], current_point["lon"])
        heading_after = calculate_heading(current_point["lat"], current_point["lon"], next_point["lat"], next_point["lon"])
        heading_change = abs(heading_after - heading_before)
        heading_change = min(heading_change, 360 - heading_change)

        # Determine if cornering event occurred
        if heading_change > 15:
            event_type = None
            if angular_velocity_magnitude_deg > hard_turn_threshold and lateral_acceleration_magnitude > hard_lateral_accel:
                event_type = "HARD_CORNER"
            elif angular_velocity_magnitude_deg > general_turn_threshold or lateral_acceleration_magnitude > general_lateral_accel:
                event_type = "GENERAL_CORNER"

            if event_type:
                start_time_unix = prev_point["timestamp"]
                end_time_unix = next_point["timestamp"]
                duration = end_time_unix - start_time_unix

                if duration <= turning_time_limit and prev_point["timestamp"] > last_event_end_time + cooldown_period:
                    events.append({
                        "event_type": event_type,
                        "start_location": (prev_point["lat"], prev_point["lon"]),
                        "end_location": (next_point["lat"], next_point["lon"]),
                        "start_time_unix": start_time_unix,
                        "end_time_unix": end_time_unix,
                        "duration": duration,
                        "angular_velocity_deg_s": round(angular_velocity_magnitude_deg, 2),
                        "lateral_acceleration_g": round(lateral_acceleration_magnitude, 3)
                    })
                    last_event_end_time = end_time_unix

    return events

def detect_cornering_events_wrapper(input_file, config=None):
    """
    Main function to detect cornering events from a given input file.
    
    Args:
        input_file (str): Path to the input file containing driving data
        config (dict): Configuration dictionary containing thresholds and settings
        
    Returns:
        list: List of detected cornering events
    """
    if config is None:
        config = {}
        
    with open(input_file, "r") as f:
        session_string = f.read()

    parsed_data = parse_session_data(session_string)
    return detect_cornering_events(parsed_data, config)

if __name__ == "__main__":
    # Run the script with an example file
    events = detect_cornering_events_wrapper("test1.txt")
    
    # Print events for standalone execution
    for event in events:
        print(f"Event: {event['event_type']}")
        print(f"  Start Location: {event['start_location']}")
        print(f"  End Location: {event['end_location']}")
        print(f"  Timestamp Start: {event['start_time_unix']}")
        print(f"  Timestamp End: {event['end_time_unix']}")
        print(f"  Duration: {event['duration']} seconds")
        print(f"  Angular Velocity: {event['angular_velocity_deg_s']} deg/s")
        print(f"  Lateral Acceleration: {event['lateral_acceleration_g']} g\n")

    print(f"Total Cornering Events Detected: {len(events)}")
