import math
from datetime import datetime

"""
Can you write me a python script that can detect general cornering and hard cornering events? The script will read in driving session data from a file. The driving session data is formatted: "lat,long,distracted,velocity,timestamp,gyro-x,gyro-y,gyro-z,accelerometer-x,accelerometer-y,accelerometer-a|...". A few things to note are each data point is separated by the "|" character and timestamp is in this format - 1746468572. I want you to write me an algorithm that can detect general cornering and hard cornering events from the driving session, and output the type of event (either general cornering and hard cornering), user's location, timestamp it occurred (readable format with hour, min, sec and as well as 1746468572 format), and duration of the event (check for consecutive data points that are considered general cornering and hard cornering events.
"""

# Thresholds
BRAKING_THRESHOLD = 9.0   # mph/s
ACCEL_THRESHOLD = 7.0     # mph/s

def g_to_mphs(g_val): # G -> mph/s
    return g_val * 9.81 * 2.237

def parse_data_point(raw):
    fields = raw.strip().split(',')
    if len(fields) < 11:
        return None
    lat = float(fields[0])
    lon = float(fields[1])
    velocity = float(fields[3])
    timestamp_raw = int(fields[4])
    accel_x = float(fields[8])
    accel_y = float(fields[9])
    accel_z = float(fields[10])

    accel_magnitude_g = math.sqrt(accel_x**2 + accel_y**2 + accel_z**2)
    accel_magnitude_mphs = g_to_mphs(accel_magnitude_g)

    return {
        "lat": lat,
        "lon": lon,
        "velocity": velocity,
        "timestamp_raw": timestamp_raw,
        "timestamp_hr": datetime.fromtimestamp(timestamp_raw).strftime('%H:%M:%S'),
        "accel_mphs": accel_magnitude_mphs
    }

# def moving_average(values, window_size=5):
#     smoothed = []
#     for i in range(len(values)):
#         window = values[max(0, i - window_size + 1):i + 1]
#         avg = sum(window) / len(window)
#         smoothed.append(avg)
#     return smoothed

def detect_events(data_points):
    events = [] # final lsit of detected events
    current_event = None # tracking of current ongoing event

    for i in range(1, len(data_points)):
        prev = data_points[i - 1]
        curr = data_points[i]

        if prev is None or curr is None:
            continue

        vel_change = curr['velocity'] - prev['velocity']
        prev_accel = prev['accel_mphs']
        curr_accel = curr['accel_mphs']
        diff_accel = curr_accel - prev_accel
        
        """
        A hard braking event is when:
            1) Acceleration is high (magnitude exceeds braking threshold).
            2) Velocity is decreasing (so the user is slowing down).

        A hard acceleration event is when:
            1) Acceleration is high (magnitude exceeds acceleration threshold).
            2) Velocity is increasing.

        - Events are ignored below 15 mph
        """
        if curr_accel > BRAKING_THRESHOLD and vel_change < 0 and curr['velocity'] > 15 and diff_accel > BRAKING_THRESHOLD:
            event_type = "Hard Braking"
        elif curr_accel > ACCEL_THRESHOLD and vel_change > 0 and curr['velocity'] > 15 and diff_accel > ACCEL_THRESHOLD:
            event_type = "Hard Acceleration"
        else:
            event_type = None

        # Check if we are in a new event
        if event_type:
            # Still in same type of event
            if current_event and current_event['type'] == event_type:
                current_event['end'] = curr
                current_event['max_accel'] = max(current_event['max_accel'], curr_accel)
            # Starting a new event
            else:
                if current_event:
                    events.append(current_event)
                current_event = {
                    "type": event_type,
                    "start": curr,
                    "end": curr,
                    "max_accel": curr_accel
                }
        # Not an event anymore
        else:
            if current_event:
                events.append(current_event)
                current_event = None

    if current_event:
        events.append(current_event)

    return events

def print_events(events):
    for e in events:
        start = e['start']
        end = e['end']
        duration = end['timestamp_raw'] - start['timestamp_raw'] + 1
        print(f"Event: {e['type']}")
        print(f"  Location: ({start['lat']}, {start['lon']})")
        print(f"  ID: {start['timestamp_raw']}")
        print(f"  Start Time: {start['timestamp_hr']}")
        print(f"  End Time: {end['timestamp_hr']}")
        print(f"  Duration: {duration} seconds")
        print(f"  Max Acceleration Magnitude: {e['max_accel']:.2f} mph/s")
        print("")

# --- Script Execution ---
with open("test2.txt", "r") as f:
    raw_data = f.read().strip()

data_strings = raw_data.split('|')
data_points = [parse_data_point(s) for s in data_strings]

### Smoothing
# raw_magnitudes = [dp['accel_mphs'] for dp in data_points]
# smoothed_magnitudes = moving_average(raw_magnitudes)
# for dp, smoothed_val in zip(data_points, smoothed_magnitudes):
#     dp['accel_mphs_smoothed'] = smoothed_val

events = detect_events(data_points)
print_events(events)
print(f" Total Number of Events Detected: {len(events)}")
print(f" Data Points Processed: {len(data_points)}")
