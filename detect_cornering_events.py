import math
from datetime import datetime, timezone

# Constants
GENERAL_TURN_THRESHOLD_DEG_S = 15 # initially 30, but lowered to account for some missed turns
HARD_TURN_THRESHOLD_DEG_S = 75

GENERAL_LATERAL_ACCEL_G = 0.1
HARD_LATERAL_ACCEL_G = 0.4

TURNING_TIME_LIMIT = 15

COOLDOWN_PERIOD = 3

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
def detect_cornering_events(data):
    events = []
    last_event_end_time = 0 # track end of last cornering event
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
            if angular_velocity_magnitude_deg > HARD_TURN_THRESHOLD_DEG_S and lateral_acceleration_magnitude > HARD_LATERAL_ACCEL_G:
                event_type = "HARD_CORNER"
            elif angular_velocity_magnitude_deg > GENERAL_TURN_THRESHOLD_DEG_S or lateral_acceleration_magnitude > GENERAL_LATERAL_ACCEL_G:
                event_type = "GENERAL_CORNER"

            if event_type:
                start_time_unix = prev_point["timestamp"]
                end_time_unix = next_point["timestamp"]
                duration = end_time_unix - start_time_unix

                start_time_readable = datetime.fromtimestamp(start_time_unix, tz=timezone.utc).isoformat()
                end_time_readable = datetime.fromtimestamp(end_time_unix, tz=timezone.utc).isoformat()

                if duration <= TURNING_TIME_LIMIT and prev_point["timestamp"] > last_event_end_time + COOLDOWN_PERIOD:
                    events.append({
                        "event_type": event_type,
                        "start_location": (prev_point["lat"], prev_point["lon"]),
                        "end_location": (next_point["lat"], next_point["lon"]),
                        "start_time_unix": start_time_unix,
                        "end_time_unix": end_time_unix,
                        "start_time_readable": start_time_readable,
                        "end_time_readable": end_time_readable,
                        "duration": duration,
                        "angular_velocity_deg_s": round(angular_velocity_magnitude_deg, 2),
                        "lateral_acceleration_g": round(lateral_acceleration_magnitude, 3)
                    })
                    last_event_end_time = end_time_unix

    return events

def start_detection():
    with open("test1.txt", "r") as f:
        session_string = f.read()

    parsed_data = parse_session_data(session_string)
    cornering_events = detect_cornering_events(parsed_data)

    for event in cornering_events:
        print(f"Event: {event['event_type']}")
        print(f"  Start Location: {event['start_location']}")
        print(f"  End Location: {event['end_location']}")
        print(f"  Start ID: {event['start_time_unix']}")
        print(f"  End ID: {event['end_time_unix']}")
        print(f"  Start Time: {event['start_time_readable']}")
        print(f"  End Time: {event['end_time_readable']}")
        print(f"  Duration: {event['duration']} seconds")
        print(f"  Angular Velocity: {event['angular_velocity_deg_s']} deg/s")
        print(f"  Lateral Acceleration: {event['lateral_acceleration_g']} g\n")

    print(f"Total Cornering Events Detected: {len(cornering_events)}")
start_detection()
