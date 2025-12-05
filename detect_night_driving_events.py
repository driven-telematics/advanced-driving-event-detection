import argparse
from datetime import datetime, timezone
from helper_functions import convert_utc_offset_to_hours, local_to_utc_hour

def parse_points(input_file):
    """
    Parse the input file into a list of points with distracted, timestamp, etc.
    Returns a list of dicts with keys: lat, lon, distracted, speed, timestamp
    """
    points = []
    with open(input_file, "r") as file:
        data = file.read().strip().split("|")
        for row in data:
            if not row:
                continue
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

def detect_night_driving_events(input_file, config=None):
    """
    Detect late night driving events between 12 AM and 4 AM local time.
    Automatically detects the system's local timezone.
    
    Args:
        input_file (str): Path to the input file containing driving data
        config (dict): Configuration dictionary (optional, timezone auto-detected)
        
    Returns:
        dict: Dictionary containing late night driving statistics
    """

    lower_bound = config.get("LOWER_BOUND_DRIVE_HOUR", 0)
    upper_bound = config.get("UPPER_BOUND_DRIVE_HOUR", 4)
    utc_offset_str = config.get('UTC_OFFSET', '-05:00:00')
    offset_hours = convert_utc_offset_to_hours(utc_offset_str)
    night_lower_utc = local_to_utc_hour(lower_bound, offset_hours)
    night_upper_utc = local_to_utc_hour(upper_bound, offset_hours)

    try:
        # Parse points from the input file
        points = parse_points(input_file)
        
        if not points:
            return {
                'error': 'No valid points found in input file',
                'total_night_driving_seconds': 0,
                'total_points': 0,
                'night_driving_points': 0
            }
        
        night_driving_seconds = 0
        night_driving_points = 0
        total_points = len(points)
        
        # Process each point to check if it's within late night hours
        for i, point in enumerate(points):
            timestamp = point['timestamp']
            
            # Convert Unix timestamp to datetime in the local timezone
            hour_utc = datetime.fromtimestamp(timestamp, tz=timezone.utc).hour 

            if night_lower_utc <= hour_utc < night_upper_utc: 
                night_driving_points += 1
                
                # Calculate seconds for this point
                # For the first point, assume 1 second duration
                # For subsequent points, calculate duration from previous point
                if i == 0:
                    point_seconds = 1
                else:
                    prev_timestamp = points[i-1]['timestamp']
                    point_seconds = timestamp - prev_timestamp
                    # Ensure we don't have negative or unreasonably large durations
                    point_seconds = max(0, min(point_seconds, 300))  # Max 5 minutes between points
                
                night_driving_seconds += point_seconds
        
        return {
            'total_night_driving_seconds': night_driving_seconds,
            'total_night_driving_minutes': round(night_driving_seconds / 60, 2),
            'total_night_driving_hours': round(night_driving_seconds / 3600, 2),
            'total_points': total_points,
            'night_driving_points': night_driving_points,
            'night_driving_percentage': round((night_driving_points / total_points) * 100, 2) if total_points > 0 else 0
        }
        
    except Exception as e:
        return {
            'error': f'Error processing night driving events: {str(e)}',
            'total_night_driving_seconds': 0,
            'total_points': 0,
            'night_driving_points': 0
        }

def detect_night_driving_events_wrapper(input_file, config=None):
    """
    Wrapper function for night events detection to match the interface
    expected by detect_all_driving_events.py
    
    Args:
        input_file (str): Path to the input file containing driving data
        config (dict): Configuration dictionary
        
    Returns:
        dict: Dictionary containing late night driving statistics
    """
    return detect_night_driving_events(input_file, config)

def main():
    """
    Main entry point for detecting late night driving events.
    Can be run independently for testing purposes.
    """
    parser = argparse.ArgumentParser(description='Detect late night driving events (12 AM - 4 AM) using local timezone')
    parser.add_argument('input_file', help='Path to the input file containing driving data')
    args = parser.parse_args()
    
    # Create config dictionary (timezone will be auto-detected)
    config = {
        'LOWER_BOUND_DRIVE_HOUR': 0,
        'UPPER_BOUND_DRIVE_HOUR': 4
    }
    
    try:
        results = detect_night_driving_events(args.input_file, config)
        
        if 'error' in results:
            print(f"Error: {results['error']}")
            return
        
        print("="*50)
        print("LATE NIGHT DRIVING EVENTS (12 AM - 4 AM)")
        print("="*50)
        print(f"Total Night Driving Time: {results['total_night_driving_seconds']} seconds")
        print(f"Total Night Driving Time: {results['total_night_driving_minutes']} minutes")
        print(f"Total Night Driving Time: {results['total_night_driving_hours']} hours")
        print(f"Points During Night Hours: {results['night_driving_points']}")
        print(f"Percentage of Points During Night: {results['night_driving_percentage']}%")
        
    except FileNotFoundError:
        print(f"Error: Input file '{args.input_file}' not found")
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
