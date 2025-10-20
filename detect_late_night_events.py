import argparse
from datetime import datetime
import pytz
import os
import time

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

def detect_late_night_events(input_file, config=None):
    """
    Detect late night driving events between 12 AM and 4 AM local time.
    Automatically detects the system's local timezone.
    
    Args:
        input_file (str): Path to the input file containing driving data
        config (dict): Configuration dictionary (optional, timezone auto-detected)
        
    Returns:
        dict: Dictionary containing late night driving statistics
    """

    lower_bound_drive_hour = int(config.get('LOWER_BOUND_DRIVE_HOUR', 0))
    upper_bound_drive_hour = int(config.get('UPPER_BOUND_DRIVE_HOUR', 4))

    try:
        # Parse points from the input file
        points = parse_points(input_file)
        
        if not points:
            return {
                'error': 'No valid points found in input file',
                'total_late_night_seconds': 0,
                'total_points': 0,
                'late_night_points': 0
            }
        
        # Automatically detect the local timezone
        try:
            
            # Try to get the system timezone using datetime.now().astimezone()
            local_dt = datetime.now()
            local_tz = local_dt.astimezone().tzinfo
            timezone = local_tz
            
            # Get timezone string for display
            timezone_str = str(timezone)
            
        except Exception as e:
            print(f"Warning: Could not detect local timezone: {e}, using UTC")
            timezone = pytz.UTC
            timezone_str = 'UTC'
        
        late_night_seconds = 0
        late_night_points = 0
        total_points = len(points)
        
        # Process each point to check if it's within late night hours
        for i, point in enumerate(points):
            timestamp = point['timestamp']
            
            # Convert Unix timestamp to datetime in the local timezone
            dt_utc = datetime.fromtimestamp(timestamp, tz=pytz.UTC)
            dt_local = dt_utc.astimezone(timezone)
            
            hour = dt_local.hour
            if lower_bound_drive_hour <= hour < upper_bound_drive_hour: 
                late_night_points += 1
                
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
                
                late_night_seconds += point_seconds
        
        return {
            'total_late_night_seconds': late_night_seconds,
            'total_late_night_minutes': round(late_night_seconds / 60, 2),
            'total_late_night_hours': round(late_night_seconds / 3600, 2),
            'total_points': total_points,
            'late_night_points': late_night_points,
            'late_night_percentage': round((late_night_points / total_points) * 100, 2) if total_points > 0 else 0,
            'timezone': timezone_str,
            'timezone_used': str(timezone)
        }
        
    except Exception as e:
        return {
            'error': f'Error processing late night events: {str(e)}',
            'total_late_night_seconds': 0,
            'total_points': 0,
            'late_night_points': 0
        }

def detect_late_night_events_wrapper(input_file, config=None):
    """
    Wrapper function for late night events detection to match the interface
    expected by detect_all_driving_events.py
    
    Args:
        input_file (str): Path to the input file containing driving data
        config (dict): Configuration dictionary
        
    Returns:
        dict: Dictionary containing late night driving statistics
    """
    return detect_late_night_events(input_file, config)

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
        results = detect_late_night_events(args.input_file, config)
        
        if 'error' in results:
            print(f"Error: {results['error']}")
            return
        
        print("="*50)
        print("LATE NIGHT DRIVING EVENTS (12 AM - 4 AM)")
        print("="*50)
        print(f"Timezone: {results['timezone_used']}")
        print(f"Total Late Night Driving Time: {results['total_late_night_seconds']} seconds")
        print(f"Total Late Night Driving Time: {results['total_late_night_minutes']} minutes")
        print(f"Total Late Night Driving Time: {results['total_late_night_hours']} hours")
        print(f"Total Points in Dataset: {results['total_points']}")
        print(f"Points During Late Night Hours: {results['late_night_points']}")
        print(f"Percentage of Points During Late Night: {results['late_night_percentage']}%")
        
    except FileNotFoundError:
        print(f"Error: Input file '{args.input_file}' not found")
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
