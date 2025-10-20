import argparse

def parse_points(input_file):
    """
    Parse the input file into a list of points with distracted, timestamp, etc.
    Returns a list of dicts with keys: lat, lon, distracted, speed, timestamp
    """
    points = []
    with open(input_file, "r") as file:
        data = file.read().strip().split("|")
        for row in data:
            parts = row.split(",")
            if len(parts) < 5:
                continue
            lat = float(parts[0])
            lon = float(parts[1])
            distracted = int(parts[2])
            speed = float(parts[3])
            timestamp = int(parts[4])
            points.append({
                'lat': lat,
                'lon': lon,
                'distracted': distracted,
                'speed': speed,
                'timestamp': timestamp
            })
    return points

def find_distracted_events(points, config=None):
    """
    Find all runs of at least min_consecutive consecutive distracted=1 events.
    A valid distracted driving event must:
    1. Have at least min_consecutive consecutive distracted points
    2. Last for at least min_duration_seconds
    3. Have average speed greater than min_speed_mph
    
    Returns a list of dicts: {start_idx, end_idx, start_time, end_time, length, avg_speed}
    """
    min_speed_mph_threshold = int(config.get('DISTRACTED_MIN_SPEED_MPH', 10))
    min_duration = int(config.get('DISTRACTED_MIN_DURATION_SECONDS', 5))
    events = []
    i = 0
    n = len(points)
    while i < n:
        if points[i]['distracted'] == 1:
            start = i
            while i < n and points[i]['distracted'] == 1:
                i += 1
            
            # Calculate event duration and average speed
            duration_seconds = points[i-1]['timestamp'] - points[start]['timestamp']
            
            # Validate event based on all criteria
            if (duration_seconds >= min_duration and points[start]['speed'] > min_speed_mph_threshold):
                events.append({
                    'start_idx': start,
                    'end_idx': i-1,
                    'start_time': points[start]['timestamp'],
                    'end_time': points[i-1]['timestamp'],
                    'length': duration_seconds
                })
        else:
            i += 1
    return events

def detect_distracted_events(input_file, config=None):
    """
    Detect distracted driving events from input file.
    
    Args:
        input_file (str): Path to the input file
        min_duration_seconds (int): Minimum duration in seconds
        min_speed_mph (float): Minimum speed in mph
    
    Returns:
        list: List of valid distracted driving events
    """
    points = parse_points(input_file)
    return find_distracted_events(points, config=config)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Detect distracted driving events from input file')
    parser.add_argument('input_file', help='Path to the input file containing driving data')
    parser.add_argument('--min-consecutive', type=int, default=3, help='Minimum consecutive distracted points to count as an event (default: 3)')
    args = parser.parse_args()

    distracted_events = detect_distracted_events(args.input_file, args.config)
    # Printing is now handled in the main script
    print(distracted_events) 