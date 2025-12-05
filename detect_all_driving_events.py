from datetime import datetime, timezone
import argparse
import concurrent.futures
import time
import os
import json
from geopy.distance import geodesic
from datetime import datetime
from dotenv import load_dotenv
from calculate_driving_score import calculate_driving_score, detect_overlapping_events
from detect_speeding_events import detect_speeding_records
from detect_cornering_events import detect_cornering_events_wrapper
from detect_accel_decel_events import detect_accel_decel_events_wrapper
from detect_distracted_events import detect_distracted_events
from detect_night_driving_events import detect_night_driving_events_wrapper
from decimal import Decimal

from helper_functions import convert_seconds, write_dict_to_json

# Load environment variables from .env file
load_dotenv()

# =====================
# CONFIGURATION FROM ENVIRONMENT VARIABLES
# =====================
def get_config():
    """
    Get configuration from environment variables with fallback defaults for development.
    
    Returns:
        dict: Configuration dictionary
    """
    config = {
        # Speeding Events
        'MAPILLARY_ACCESS_TOKEN': os.environ.get('MAPILLARY_ACCESS_TOKEN', ''),
        'MAPQUEST_API_KEY': os.environ.get('MAPQUEST_API_KEY', ''),
        'DRIVEN_OVERPASS_URL': os.environ.get('DRIVEN_OVERPASS_URL', 'https://maps.driven-api.com/api/interpreter'),
        'BATCH_SIZE': int(os.environ.get('BATCH_SIZE', '20')),
        'DB_BATCH_SIZE': int(os.environ.get('DB_BATCH_SIZE', '25')),
        'DYNAMODB_ROAD_SEGMENT_TABLE': os.environ.get('DYNAMODB_ROAD_SEGMENT_TABLE', 'drivenDB_road_segment_info'),
        'DYNAMODB_SPEEDING_EVENTS_TABLE': os.environ.get('DYNAMODB_SPEEDING_EVENTS_TABLE', 'users_speeding_events'),
        'USER_ID': int(os.environ.get('USER_ID', '31399')),  # Example user ID for testing

        # Speeding Events
        'EXCESS_SPEED_THRESHOLD_MPH': int(os.environ.get('EXCESS_SPEED_THRESHOLD_MPH', '11')),
        'EXCESS_SPEED_DURATION_SECONDS': int(os.environ.get('EXCESS_SPEED_DURATION_SECONDS', '5')),

        # Road Types
        'ROAD_CLASSIFICATIONS': os.environ.get('ROAD_CLASSIFICATIONS', '').split(','),

        # Cornering Events
        'GENERAL_TURN_THRESHOLD_DEG_S': float(os.environ.get('GENERAL_TURN_THRESHOLD_DEG_S', '15')),
        'HARD_TURN_THRESHOLD_DEG_S': float(os.environ.get('HARD_TURN_THRESHOLD_DEG_S', '75')),
        'GENERAL_LATERAL_ACCEL_G': float(os.environ.get('GENERAL_LATERAL_ACCEL_G', '0.1')),
        'HARD_LATERAL_ACCEL_G': float(os.environ.get('HARD_LATERAL_ACCEL_G', '0.4')),
        'TURNING_TIME_LIMIT': int(os.environ.get('TURNING_TIME_LIMIT', '15')),
        'COOLDOWN_PERIOD': int(os.environ.get('COOLDOWN_PERIOD', '3')),

        # Accel/Decel Events
        'BRAKING_THRESHOLD': float(os.environ.get('BRAKING_THRESHOLD', '9.0')),   # mph/s
        'ACCEL_THRESHOLD': float(os.environ.get('ACCEL_THRESHOLD', '7.0')),     # mph/s

        # Distracted Events
        'DISTRACTED_MIN_DURATION_SECONDS': int(os.environ.get('DISTRACTED_MIN_DURATION_SECONDS', '5')),
        'DISTRACTED_MIN_SPEED_MPH': float(os.environ.get('DISTRACTED_MIN_SPEED_MPH', '10.0')),

        # Event Detection Toggles
        'ENABLE_SPEEDING': os.environ.get('ENABLE_SPEEDING', 'true').lower() == 'true',
        'ENABLE_ROAD_FAMILIARITY': os.environ.get('ENABLE_ROAD_FAMILIARITY', 'true').lower() == 'true',
        'ENABLE_ROAD_TYPES': os.environ.get('ENABLE_ROAD_TYPES', 'true').lower() == 'true',
        'ENABLE_CORNERING': os.environ.get('ENABLE_CORNERING', 'true').lower() == 'true',
        'ENABLE_HARD_BRAKING': os.environ.get('ENABLE_HARD_BRAKING', 'true').lower() == 'true',
        'ENABLE_RAPID_ACCELERATION': os.environ.get('ENABLE_RAPID_ACCELERATION', 'true').lower() == 'true',
        'ENABLE_DISTRACTED': os.environ.get('ENABLE_DISTRACTED', 'true').lower() == 'true',
        'ENABLE_NIGHT_DRIVING': os.environ.get('ENABLE_NIGHT_DRIVING', 'true').lower() == 'true',
        
        # Trip Summary Configuration
        'ACCOUNT_ID': os.environ.get('ACCOUNT_ID', '19857054769'),
        'UTC_OFFSET': os.environ.get('UTC_OFFSET', '-04:00:00'),
        'CLASSIFICATION': os.environ.get('CLASSIFICATION', 'car'),
        'DRIVE_ID': os.environ.get('DRIVE_ID', '20251021/12300_12300_00001'),
        'DEVICE_ID': os.environ.get('DEVICE_ID', 'CEE06157DECA4099'),
    }
    
    # --- Base Penalty Values ---
    config['penalty_values'] = {
        'distracted': float(os.environ.get('PENALTY_DISTRACTED', '26.0')),
        'speeding': float(os.environ.get('PENALTY_SPEEDING', '26.0')),
        'hard_braking': float(os.environ.get('PENALTY_HARD_BRAKING', '600.0')),
        'rapid_acceleration': float(os.environ.get('PENALTY_RAPID_ACCELERATION', '600.0')),
        'cornering': float(os.environ.get('PENALTY_CORNERING', '1200.0')),
        'night_driving': float(os.environ.get('PENALTY_NIGHT_DRIVING', '2.0')),
        'road_familiarity': float(os.environ.get('PENALTY_ROAD_FAMILIARITY', '1.05')),
        'road_type': float(os.environ.get('PENALTY_ROAD_TYPE', '1.05')),
    }

    config['speeding_thresholds'] = {
        'max_allowed_speed_threshold': float(os.environ.get('MAX_ALLOWED_SPEED_THRESHOLD', '55.0')),
        'severe_speed_threshold': float(os.environ.get('SEVERE_SPEED_THRESHOLD', '20.0')),
    }

    # --- Behavior Factors ---
    config['behavior_factors'] = {
        'distracted': {
            'speeding_over_threshold': float(os.environ.get('BF_DISTRACTED_SPEEDING_OVER_THRESHOLD', '2.5')),
            'hard_braking': float(os.environ.get('BF_DISTRACTED_HARD_BRAKING', '2.0')),
            'night_driving': float(os.environ.get('BF_DISTRACTED_NIGHT_DRIVING', '3.0')),
        },
        'speeding': {
            'distracted': float(os.environ.get('BF_SPEEDING_DISTRACTED', '2.5')),
            'hard_braking': float(os.environ.get('BF_SPEEDING_HARD_BRAKING', '2.0')),
            'night_driving': float(os.environ.get('BF_SPEEDING_NIGHT_DRIVING', '2.5')),
            'excess_speeding': float(os.environ.get('BF_SPEEDING_EXCESS_SPEEDING', '2.5')),
        },
        'hard_braking': {
            'distracted': float(os.environ.get('BF_HARD_BRAKING_DISTRACTED', '2.0')),
            'speeding': float(os.environ.get('BF_HARD_BRAKING_SPEEDING', '2.0')),
            'night_driving': float(os.environ.get('BF_HARD_BRAKING_NIGHT_DRIVING', '2.5')),
        },
        'rapid_acceleration': {
            'speeding': float(os.environ.get('BF_RAPID_ACCELERATION_SPEEDING', '1.5')),
            'night_driving': float(os.environ.get('BF_RAPID_ACCELERATION_NIGHT_DRIVING', '3.0')),
        },
        'cornering': {
            'night_driving': float(os.environ.get('BF_CORNERING_NIGHT_DRIVING', '3.0')),
        }
    }

    # --- Weights ---
    config['weights'] = {
        'distracted': float(os.environ.get('WEIGHT_DISTRACTED', '0.38')),
        'speeding': float(os.environ.get('WEIGHT_SPEEDING', '0.25')),
        'hard_braking': float(os.environ.get('WEIGHT_HARD_BRAKING', '0.18')),
        'rapid_acceleration': float(os.environ.get('WEIGHT_RAPID_ACCELERATION', '0.08')),
        'cornering': float(os.environ.get('WEIGHT_CORNERING', '0.04')),
        'night_driving': float(os.environ.get('WEIGHT_NIGHT_DRIVING', '0.04')),
        'road_familiarity': float(os.environ.get('WEIGHT_ROAD_FAMILIARITY', '0.015')),
        'road_type': float(os.environ.get('WEIGHT_ROAD_TYPE', '0.015')),
    }

    if not config['ENABLE_SPEEDING']:
        config['ENABLE_ROAD_FAMILIARITY'] = False
        config['ENABLE_ROAD_TYPES'] = False
    
    return config

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

def convert_timestamp(timestamp, format):
    timestamp = int(timestamp)
    if format == 'strftime':
        return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    elif format == 'datetime':
        return datetime.fromtimestamp(timestamp).isoformat() + "Z"
    elif format == 'epoch':
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        return int(dt.timestamp())

def calculate_distance_and_duration(points):
    """
    Calculate total distance (in miles) and duration (in seconds) from GPS data.
    
    :param points: List of tuples [(lat, lon, distracted, speed, timestamp), ...]
    :return: Total distance (miles), total duration (seconds)
    """
    if len(points) < 2:
        return 0, 0  # Not enough data points

    # Extract relevant fields and sort by timestamp
    parsed_data = [(p['lat'], p['lon'], p['distracted'], p['speed'], p['timestamp']) for p in points]
    parsed_data.sort(key=lambda x: x[4])  # Sort by timestamp (if not already sorted)

    total_distance = 0
    start_time = parsed_data[0][4]
    end_time = parsed_data[-1][4]

    for i in range(1, len(parsed_data)):
        point1 = (parsed_data[i - 1][0], parsed_data[i - 1][1])
        point2 = (parsed_data[i][0], parsed_data[i][1])
        total_distance += geodesic(point1, point2).miles  # Compute great-circle distance

    total_seconds = end_time - start_time
    # minutes = total_seconds // 60
    # seconds = total_seconds % 60

    return total_distance, total_seconds

def determine_events_to_process(config):
    """
    Determine which events to process based on configuration flags.
    
    Args:
        config (dict): Configuration dictionary
        
    Returns:
        tuple: (events_to_process_list, enabled_events_dict)
    """
    enabled_events = {
        'speeding': config.get('ENABLE_SPEEDING', True),
        'road_familiarity': config.get('ENABLE_ROAD_FAMILIARITY', True),
        'road_types': config.get('ENABLE_ROAD_TYPES', True),
        'cornering': config.get('ENABLE_CORNERING', True),
        'hard_braking': config.get('ENABLE_HARD_BRAKING', True),
        'rapid_acceleration': config.get('ENABLE_RAPID_ACCELERATION', True),
        'distracted': config.get('ENABLE_DISTRACTED', True),
        'night_driving': config.get('ENABLE_NIGHT_DRIVING', True),
    }
    
    # Convert to list of enabled events for processing
    events_to_process = []
    if enabled_events['speeding']:
        events_to_process.append('speeding')
    if enabled_events['cornering']:
        events_to_process.append('cornering')
    if enabled_events['hard_braking'] or enabled_events['rapid_acceleration']:
        events_to_process.append('accel_decel')
    if enabled_events['distracted']:
        events_to_process.append('distracted')
    if enabled_events['night_driving']:
        events_to_process.append('night_driving')
    
    return events_to_process, enabled_events

def print_speeding_records(results):
    """
    Print speeding records and detailed information in a formatted way.
    
    Args:
        results (dict): Results from speeding events detection
    """
    if 'error' in results:
        print(f"Error in speeding detection: {results['error']}")
        return
        
    geocode_to_segment = results.get('geocode_to_segment', {})
    travelled_segments = results.get('travelled_segments', {})
    
    print("\n" + "="*50)
    print("SPEEDING RECORDS DETAILS")
    print("="*50)
    
    for (lat, lon, distracted, traveling_speed, timestamp), segment_data in geocode_to_segment.items():
        segment_id = segment_data['segment_id']
        distance_meters = segment_data['distance_meters']
        segment = travelled_segments[segment_id]
        
        osm_speed_limit = segment['osm_speed_limit'] if segment['osm_speed_limit'] and segment['osm_speed_limit'] != 'Unknown' else 0
        mapillary_speed_limit = segment['mapillary_speed_limit'] if segment['mapillary_speed_limit'] > 0 else 0
        segment_average_speed = segment['avg_traveling_speed'] if segment['avg_traveling_speed'] > 0 else 0

        print(f"⏱️  Timestamp: {convert_timestamp(timestamp, 'strftime')}")
        print(f"📍 Location: {lat}, {lon}")
        print(f"📏  Distance from Driver to Nearest Road: {distance_meters} m")
        print(f"🚧 Road Segment ID: {segment['id'] if segment else 'None'}")
        print(f"🛣️  Road Segment: {segment['road_name']}")
        print(f"🚧  Road Type: {segment['road_type']}")
        print(f"🚦 OSM Speed Limit: {osm_speed_limit} mph")
        print(f"🚦 Mapillary Speed Limit: {mapillary_speed_limit} mph")
        print(f"🚦 Average Segment Traveling Speed: {segment_average_speed} mph")
        print(f"🚗 Traveling Speed: {traveling_speed} mph\n")
    
def print_speeding_service_metrics(metrics):
    # Print metrics
    print("======= PERFORMANCE METRICS =======")
    print(f"# of segments with unknown speeds: {metrics.get('unknown_speeds', 0)}")
    print(f"# of OSM API calls: {metrics.get('osm_api_calls', 0)}")
    print(f"# of Mapillary speed signs: {metrics.get('mapillary_speed_signs', 0)}")
    print(f"# of MapQuest API calls: {metrics.get('mapquest_api_calls', 0)}")
    
    timings = metrics.get('timings', {})
    print(f"Time to complete reading file: {timings.get('reading_file', 0):.4f} seconds")
    print(f"Time to complete Mapillary API call: {timings.get('mapillary_api', 0):.4f} seconds")
    print(f"Time to complete determine_travelled_segments: {timings.get('determine_segments', 0):.4f} seconds")
    print(f"Time to complete find_speed_limits: {timings.get('resolve_speed_limits', 0):.4f} seconds")
    print(f"Time to complete final_output_functionality: {timings.get('final_output', 0):.4f} seconds")
    print(f"Time to complete full algorithm: {timings.get('total', 0):.4f} seconds")

def print_grouped_speeding_events(grouped_events):
    print("\n======= GROUPED SPEEDING EVENTS (10+ mph over for 5+ seconds) =======")
    print("\n Total Speeding Events Detected:", len(grouped_events))
    for idx, event in enumerate(grouped_events, 1):
        print(f"Event {idx}:")
        print(f"  Start Time: {datetime.fromtimestamp(event['start_time'])}")
        print(f"  End Time: {datetime.fromtimestamp(event['end_time'])}")
        print(f"  Duration: {event['duration']} seconds")
        print(f"  Road Type: {event['road_type']}")
        print(f"  Start Speed: {event['start_speed']} mph")
        print(f"  End Speed: {event['end_speed']} mph")
        print(f"  Start Speed Deviation: {event['driver_speed_deviation_start']} mph")
        print(f"  End Speed Deviation: {event['driver_speed_deviation_end']} mph")
        print(f"  # Points: {len(event)}")
        print("  Details:")
        for point in event['points']:
            print(f"    Time: {datetime.fromtimestamp(point['timestamp'])}, Speed: {point['speed']} mph, Road Type: {point['road_type']}, Location: ({point['lat']}, {point['long']})")
            print(f"    Segment Traveled On: {point['segment_id']}")
            print(f"    Avg Segment Traveling Speed: {point['avg_segment_traveling_speed']} mph")
            print(f"    Avg Segment Speed Deviation: {point['avg_speed_deviation']} mph")
            print(f"    Driver Speed Deviation: {point['driver_speed_deviation']} mph")
        print("-" * 50)

def print_cornering_events(events):
    """
    Print cornering events in a formatted way.
    
    Args:
        events (list): List of cornering events
    """
    for event in events:
        print(f"Event: {event['event_type']}")
        print(f"  Start Location: {event['start_location']}")
        print(f"  End Location: {event['end_location']}")
        print(f"  Start Time: {convert_timestamp(event['start_time_unix'], 'datetime')}")
        print(f"  End Time: {convert_timestamp(event['end_time_unix'], 'datetime')}")
        print(f"  Timestamp Start: {event['start_time_unix']}")
        print(f"  Timestamp End: {event['end_time_unix']}")
        print(f"  Duration: {event['duration']} seconds")
        print(f"  Angular Velocity: {event['angular_velocity_deg_s']} deg/s")
        print(f"  Lateral Acceleration: {event['lateral_acceleration_g']} g\n")

def print_accel_decel_events(events):
    """
    Print acceleration/deceleration events in a formatted way.
    
    Args:
        events (list): List of acceleration/deceleration events
    """
    for event in events:
        start = event['start']
        end = event['end']
        duration = end['timestamp'] - start['timestamp'] + 1
        print(f"Event: {event['type']}")
        print(f"  Location: ({start['lat']}, {start['lon']})")
        print(f"  Start Time: {convert_timestamp(start['timestamp'], 'datetime')}")
        print(f"  End Time: {convert_timestamp(end['timestamp'], 'datetime')}")
        print(f"  Duration: {duration} seconds")
        print(f"  Max Acceleration Magnitude: {event['max_accel']:.2f} mph/s\n")

def print_distracted_events(distracted_events):
    print("\n======= DISTRACTED EVENTS =======")
    print(f"Total Distracted Events: {len(distracted_events)}")
    for i, event in enumerate(distracted_events, 1):
        print(f"Event {i}: Start idx {event['start_idx']}, End idx {event['end_idx']}, Start time {event['start_time']}, End time {event['end_time']}, Length {event['length']}")

def print_night_driving_events(night_driving_results):
    """
    Print night driving events in a formatted way.
    
    Args:
        night_driving_results (dict): Results from night events detection
    """
    if 'error' in night_driving_results:
        print(f"Error in night driving detection: {night_driving_results['error']}")
        return
        
    print("\n" + "="*50)
    print("NIGHT DRIVING EVENTS (12 AM - 4 AM)")
    print("="*50)
    print(f"Total Night Driving Time: {night_driving_results['total_night_driving_seconds']} seconds")
    print(f"Total Night Driving Time: {night_driving_results['total_night_driving_minutes']} minutes")
    print(f"Total Night Driving Time: {night_driving_results['total_night_driving_hours']} hours")
    print(f"Points During Night Hours: {night_driving_results['night_driving_points']}")
    print(f"Percentage of Points During Night: {night_driving_results['night_driving_percentage']}%")

def print_road_history_stats(road_history_stats):
    """
    Print road segment history statistics in a formatted way.
    
    Args:
        road_history_stats (dict): Results from road segment history tracking
    """

    if 'error' in road_history_stats:
        print(f"Error in road history tracking: {road_history_stats['error']}")
        return

    print("\n" + "="*50)
    print("ROAD SEGMENT HISTORY TRACKING (21-DAY WINDOW)")
    print("="*50)
    print(f"Total Segments in Current Trip: {road_history_stats['total_segments']}")
    print(f"Segments Driven Recently (within 21 days): {road_history_stats['segments_driven_recently']}")
    print(f"Segments Not Driven Recently (new or expired): {road_history_stats['segments_not_driven_recently']}")
    
    # Calculate percentage of segments driven recently
    if road_history_stats['total_segments'] > 0:
        recent_percentage = (road_history_stats['segments_driven_recently'] / road_history_stats['total_segments']) * 100
        print(f"Percentage of Segments Driven Recently: {recent_percentage:.1f}%")
    
    # Print segment IDs if available and not too many
    if 'segments_driven_recently_ids' in road_history_stats and road_history_stats['segments_driven_recently'] <= 10:
        print(f"Recently Driven Segment IDs: {road_history_stats['segments_driven_recently_ids']}")
    elif 'segments_driven_recently_ids' in road_history_stats:
        print(f"Recently Driven Segment IDs: {len(road_history_stats['segments_driven_recently_ids'])} segments (too many to display)")
    
    if 'segments_not_driven_recently_ids' in road_history_stats and road_history_stats['segments_not_driven_recently'] <= 10:
        print(f"New Segment IDs: {road_history_stats['segments_not_driven_recently_ids']}")
    elif 'segments_not_driven_recently_ids' in road_history_stats:
        print(f"New Segment IDs: {len(road_history_stats['segments_not_driven_recently_ids'])} segments (too many to display)")

def print_road_types_travelled(road_types_travelled):
    """
    Print road type statistics in a formatted way.
    
    Args:
        road_types_travelled (dict): Results from road type tracking
    """

    if 'error' in road_types_travelled:
        print(f"Error in road type tracking: {road_types_travelled['error']}")
        return

    print("\n" + "="*50)
    print("ROAD TYPES TRAVELLED")
    print("="*50)
    # Extract total count if present
    total_count = road_types_travelled.get('road_types_travelled_count', 0)

    # Print each road type count, excluding the total key
    for road_type, count in sorted(road_types_travelled.items()):
        if road_type == 'road_types_travelled_count':
            continue
        print(f"{road_type:<25} : {count}")

    print("-" * 50)
    print(f"{'TOTAL':<25} : {total_count}")
    print("=" * 50 + "\n")
    

def run_speeding_detection(input_file):
    """
    Wrapper function for speeding detection to be used with ThreadPoolExecutor.
    
    Args:
        input_file (str): Path to the input file
        
    Returns:
        tuple: (event_type, results)
    """
    try:
        config = get_config()
        results = detect_speeding_records(input_file, config=config)
        return ('speeding', results)
    except Exception as e:
        return ('speeding', {'error': str(e)})

def run_cornering_detection(input_file):
    """
    Wrapper function for cornering detection to be used with ThreadPoolExecutor.
    
    Args:
        input_file (str): Path to the input file
        
    Returns:
        tuple: (event_type, results)
    """
    try:
        config = get_config()
        results = detect_cornering_events_wrapper(input_file, config=config)
        return ('cornering', results)
    except Exception as e:
        return ('cornering', {'error': str(e)})

def run_accel_decel_detection(input_file):
    """
    Wrapper function for acceleration/deceleration detection with individual toggles.
    
    Args:
        input_file (str): Path to the input file
        config (dict): Configuration dictionary
        enabled_events (dict): Dictionary of enabled events
        
    Returns:
        tuple: (event_type, results)
    """
    try:
        config = get_config()
        # Get all accel/decel events
        all_events = detect_accel_decel_events_wrapper(input_file, config=config)
        
        # Filter based on enabled events
        filtered_events = []
        for event in all_events:
            if event['type'] == 'Hard Braking' and config.get('hard_braking', True):
                filtered_events.append(event)
            elif event['type'] == 'Rapid Acceleration' and config.get('rapid_acceleration', True):
                filtered_events.append(event)
        
        return ('accel_decel', filtered_events)
    except Exception as e:
        return ('accel_decel', {'error': str(e)})

def run_distracted_detection(input_file, config=None):
    """
    Wrapper function for distracted detection to be used with ThreadPoolExecutor.
    Args:
        input_file (str): Path to the input file
        min_consecutive (int): Minimum consecutive distracted points
        config (dict): Configuration dictionary with distracted driving parameters
    Returns:
        tuple: (event_type, results)
    """
    try:
        config = get_config()
        results = detect_distracted_events(input_file, config=config)
        return ('distracted', results)
    except Exception as e:
        return ('distracted', {'error': str(e)})

def run_night_driving_detection(input_file):
    """
    Wrapper function for night_driving detection to be used with ThreadPoolExecutor.
    
    Args:
        input_file (str): Path to the input file
        
    Returns:
        tuple: (event_type, results)
    """
    try:
        config = get_config()
        results = detect_night_driving_events_wrapper(input_file, config=config)
        return ('night_driving', results)
    except Exception as e:
        return ('night_driving', {'error': str(e)})

def convert_decimals(obj):
    if isinstance(obj, list):
        return [convert_decimals(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        return float(obj)
    else:
        return obj

def extract_waypoints_from_results(results):
    """
    Extract waypoints from all event detection results.
    
    Args:
        results (dict): Results from all event detection
        
    Returns:
        list: List of waypoint dictionaries
    """
    waypoints = []
    
    # Use speeding results if available (most comprehensive data)
    if 'speeding' in results and 'error' not in results['speeding']:
        speeding_data = results['speeding']
        geocode_to_segment = speeding_data.get('geocode_to_segment', {})
        travelled_segments = speeding_data.get('travelled_segments', {})
        
        for (lat, lon, distracted, traveling_speed, timestamp), segment_data in geocode_to_segment.items():
            segment_id = segment_data['segment_id']
            segment = travelled_segments[segment_id]
            segment_avg_traveling_speed = segment.get('avg_traveling_speed', 0)
            segment_avg_speed_deviation = segment.get('avg_speed_deviation', 0)
            
            waypoint = {
                "lat": lat,
                "lon": lon,
                "timestamp": timestamp,
                "traveling_speed_mph": traveling_speed,
                "segment_avg_traveling_speed": segment_avg_traveling_speed,
                "segment_avg_speed_deviation": segment_avg_speed_deviation,
                "road_name": segment.get('road_name', 'Unknown'),
                "road_type": segment.get('road_type', 'Unknown'),
                "distance_meters": segment_data.get('distance_meters', 0)
            }
            waypoints.append(waypoint)
    
    return waypoints

def format_events_for_summary(results, config):
    """
    Format detected events for the trip summary.
    
    Args:
        results (dict): Results from all event detection
        config (dict): Configuration dictionary
        
    Enum Set:
        0: "general_cornering
        1: "speeding",
        2: "hard_braking", 
        3: "rapid_acceleration",
        4: "hard_cornering",
        5: "distracted_driving",
        6: "night_driving_driving"    
    
    Returns:
        list: Formatted events list
    """
    enabled_events = {
        'speeding': config.get('ENABLE_SPEEDING', True),
        'road_familiarity': config.get('ENABLE_ROAD_FAMILIARITY', True),
        'road_types': config.get('ENABLE_ROAD_TYPES', True),
        'cornering': config.get('ENABLE_CORNERING', True),
        'hard_braking': config.get('ENABLE_HARD_BRAKING', True),
        'rapid_acceleration': config.get('ENABLE_RAPID_ACCELERATION', True),
        'distracted': config.get('ENABLE_DISTRACTED', True),
        'night_driving': config.get('ENABLE_NIGHT_DRIVING', True),
    }

    events = []
    
    # Speeding Events
    if enabled_events['speeding']:
        if 'speeding' in results and 'error' not in results['speeding']:
            speeding_data = results['speeding']
            if 'grouped_events' in speeding_data:
                for i, event in enumerate(speeding_data['grouped_events']):
                    if not event:
                        continue
                    
                    # Get road history stats
                    road_history_stats = speeding_data.get('road_history_stats', {})
                    
                    event_data = {
                        "event_type": 1,  # speeding
                        "start_time": convert_timestamp(event['start_time'], 'datetime'),
                        "end_time": convert_timestamp(event['end_time'], 'datetime'),
                        "traveling_speed_mph": event['start_speed'],
                        "duration_sec": event['duration'],
                        "details": {
                            "road_type": event.get('road_type', 'Unknown'),
                        }
                    }

                    if enabled_events['road_familiarity']:
                        event_data["details"]["road_history_stats"] = {
                            "total_segments": road_history_stats.get('total_segments', 0),
                            "segments_driven_recently": road_history_stats.get('segments_driven_recently', 0),
                            "segments_not_driven_recently": road_history_stats.get('segments_not_driven_recently', 0)
                        }

                    # Get road types travelled on 
                    road_types_travelled = speeding_data.get('road_types_travelled', {})

                    if enabled_events['road_types']:
                        event_data["details"]["road_types_travelled"] = road_types_travelled

                    events.append(event_data)
    
    # Cornering Events
    if enabled_events['cornering']:
        if 'cornering' in results and 'error' not in results['cornering']:
            for event in results['cornering']:
                event_type = 4 if event['event_type'] == 'Hard Cornering' else 0  # hard_cornering or general_cornering
                
                event_data = {
                    "event_type": event_type,
                    "start_location": event['start_location'],
                    "end_location": event['end_location'],
                    "start_time": convert_timestamp(event['start_time_unix'], 'datetime'),
                    "end_time": convert_timestamp(event['end_time_unix'], 'datetime'),
                    "duration_sec": event['duration'],
                    "details": {
                        "angular_velocity_deg_s": event['angular_velocity_deg_s'],
                        "lateral_acceleration_g": event['lateral_acceleration_g'],
                    }
                }
                events.append(event_data)
    
    # Acceleration/Deceleration Events
    if enabled_events['hard_braking'] or enabled_events['rapid_acceleration']:
        if 'accel_decel' in results and 'error' not in results['accel_decel']:
            for event in results['accel_decel']:
                event_name = event.get('type')

                # Determine event type ID
                if event_name == 'Hard Braking':
                    if not enabled_events.get('hard_braking'):
                        continue  # Skip if not enabled
                    event_type = 2
                elif event_name == 'Rapid Acceleration':
                    if not enabled_events.get('rapid_acceleration'):
                        continue  # Skip if not enabled
                    event_type = 3
                else:
                    continue  # Unknown event type, skip

                start = event['start']
                end = event['end']
                duration_sec = end['timestamp'] - start['timestamp'] + 1
                
                event_data = {
                    "event_type": event_type,
                    "start_lat": start['lat'],
                    "start_lon": start['lon'],
                    "end_lat": end['lat'],
                    "end_lon": end['lon'],
                    "start_time": convert_timestamp(start['timestamp'], 'datetime'),
                    "end_time": convert_timestamp(end['timestamp'], 'datetime'),
                    "duration_sec": duration_sec,
                    "details": {
                        "max_acceleration_mph_s": event['max_accel']
                    }
                }
                events.append(event_data)
    
    # Distracted Driving Events
    if enabled_events['distracted']:
        if 'distracted' in results and 'error' not in results['distracted']:
            for event in results['distracted']:
                event_data = {
                    "event_type": 5,  # distracted_driving
                    "start_time": convert_timestamp(event['start_time'], 'datetime'),
                    "end_time": convert_timestamp(event['end_time'], 'datetime'),
                    "duration_sec": event['length']
                }
                events.append(event_data)
    
    # Night Driving Events
    if enabled_events['night_driving']:
        if 'night_driving' in results and 'error' not in results['night_driving']:
            night_driving_results = results['night_driving']
            if night_driving_results['total_night_driving_seconds'] > 0:
                event_data = {
                    "event_type": 6,  # night_driving_driving
                    "total_night_driving_seconds": night_driving_results['total_night_driving_seconds'],
                    "total_night_driving_minutes": night_driving_results['total_night_driving_minutes'],
                    "total_night_driving_hours": night_driving_results['total_night_driving_hours'],
                    "night_driving_percentage": night_driving_results['night_driving_percentage']
                }
                events.append(event_data)
    
    return events

def generate_trip_summary(results, user_data_points, config, enabled_events):
    """
    Generate a comprehensive trip summary JSON file.
    
    Args:
        results (dict): Results from all event detection
        config (dict): Configuration dictionary
        
    Returns:
        dict: Trip summary dictionary
    """
    # Extract waypoints from all event detection results

    if enabled_events.get('speeding'):
        waypoints = extract_waypoints_from_results(results)
    else:
        waypoints = user_data_points

    if not waypoints:
        return {"error": "No valid waypoints found in event detection results"}
    
    # Assign start and end points
    start_point = waypoints[0]
    end_point = waypoints[-1]
    
    # Get configuration values from environment
    drive_id = config.get('DRIVE_ID', '')
    device_id = config.get('DEVICE_ID', '')
    account_id = config.get('ACCOUNT_ID', '19857054769')
    utc_offset = config.get('UTC_OFFSET', '-04:00:00')
    classification = config.get('CLASSIFICATION', 'car')
    
    # Validate required environment variables
    if not drive_id:
        return {"error": "DRIVE_ID not set in environment variables"}
    if not device_id:
        return {"error": "DEVICE_ID not set in environment variables"}
    
    # Format events
    events = format_events_for_summary(results, config)
    
    # Create comprehensive summary
    summary = {
        "total_events": 0,
    }
    
    # Populate summary from results
    if enabled_events['speeding'] and 'speeding' in results and 'error' not in results['speeding']:
        speeding_data = results['speeding']
        summary['speeding_events'] = len(speeding_data.get('grouped_events', []))
        
        # Road history stats
        if enabled_events['road_familiarity']:
            road_history = speeding_data.get('road_history_stats', {})
            if 'error' not in road_history:
                summary['road_segments_total'] = road_history.get('total_segments', 0)
                summary['road_segments_recent'] = road_history.get('segments_driven_recently', 0)
                summary['road_segments_new'] = road_history.get('segments_not_driven_recently', 0)

        # Road Types Travelled
        # Road history stats
        if enabled_events['road_types']:
            road_types_travelled = speeding_data.get('road_types_travelled', {})
            if 'error' not in road_history:
                summary['road_types_travelled'] = road_types_travelled
    
    if enabled_events['cornering'] and 'cornering' in results and 'error' not in results['cornering']:
        summary['cornering_events'] = len(results['cornering'])
    
    if (enabled_events['hard_braking'] or enabled_events['rapid_acceleration']) and 'accel_decel' in results and 'error' not in results['accel_decel']:
        accel_decel_events = results['accel_decel']
        hard_braking_count = 0
        rapid_accel_count = 0

        for e in accel_decel_events:
            etype = e.get('type', '')
            if etype == 'Hard Braking':
                hard_braking_count += 1
            elif etype == 'Rapid Acceleration':
                rapid_accel_count += 1

        if enabled_events['hard_braking']:
            summary['hard_braking_events'] = hard_braking_count
        if enabled_events['rapid_acceleration']:
            summary['rapid_acceleration_events'] = rapid_accel_count
    
    if enabled_events['distracted'] and 'distracted' in results and 'error' not in results['distracted']:
        summary['distracted_events'] = len(results['distracted'])
    
    if enabled_events['night_driving'] and 'night_driving' in results and 'error' not in results['night_driving']:
        summary['night_driving_seconds'] = results['night_driving'].get('total_night_driving_seconds', 0)
    
    summary['total_events'] = (
        summary.get('speeding_events', 0) +
        summary.get('cornering_events', 0) +
        summary.get('hard_braking_events', 0) +
        summary.get('rapid_acceleration_events', 0) +
        summary.get('distracted_events', 0)
    )

    total_distance, total_seconds = calculate_distance_and_duration(user_data_points)
    
    # Create the trip summary
    trip_summary = {
        "driveid": drive_id,
        "distance_miles": total_distance,
        "duration_seconds": total_seconds,
        "driving": True,
        "id": int(datetime.now().timestamp()),  # Use current timestamp as ID
        "deviceid": device_id,
        "utc_offset": utc_offset,
        "classification": classification,
        "start": {
            "ts": convert_timestamp(start_point['timestamp'], 'datetime'),
            "lat": start_point['lat'],
            "lon": start_point['lon']
        },
        "end": {
            "ts": convert_timestamp(end_point['timestamp'], 'datetime'),
            "lat": end_point['lat'],
            "lon": end_point['lon']
        },
        "night_driving_seconds": summary['night_driving_seconds'] if 'night_driving_seconds' in summary else 0,
        "account_id": account_id,
        "events": events,
        "waypoints": waypoints,
        "summary": summary
    }
    
    return trip_summary

def save_trip_summary(trip_summary, output_file=None):
    """
    Save trip summary to JSON file.
    
    Args:
        trip_summary (dict): Trip summary dictionary
        output_file (str): Output file path (optional)
        
    Returns:
        str: Path to saved file
    """
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"trip_summary_{timestamp}.json"
    
    # Convert any Decimal objects to float for JSON serialization
    trip_summary_clean = convert_decimals(trip_summary)
    
    with open(output_file, 'w') as f:
        json.dump(trip_summary_clean, f, indent=2)
    
    return output_file
    

def main():
    """
    Main entry point for detecting all driving events.
    Processes command line arguments and calls the appropriate detection functions in parallel.
    """
    parser = argparse.ArgumentParser(description='Detect all driving events from input file')
    parser.add_argument('input_file', help='Path to the input file containing driving data')
    parser.add_argument('--events', nargs='+', choices=['speeding', 'cornering', 'accel_decel', 'distracted', 'night_driving', 'all'], 
                       default=['all'], help='Types of events to detect')
    parser.add_argument('--max-workers', type=int, default=3, 
                       help='Maximum number of parallel workers (default: 3)')
    parser.add_argument('--min-distracted', type=int, default=3, help='Minimum consecutive distracted points (default: 3)')
    parser.add_argument('--output-summary', type=str, default=None, 
                       help='Output file path for trip summary JSON (default: auto-generated)')
    args = parser.parse_args()

    try:
        # Get configuration and determine which events to process
        config = get_config()
        events_to_process, enabled_events = determine_events_to_process(config)
        
        # Override with command line arguments if provided (for backward compatibility)
        if 'all' not in args.events:
            events_to_process = args.events

        print(f"Processing events: {', '.join(events_to_process)}")
        print(f"Event configuration:")
        for event, enabled in enabled_events.items():
            status = "✓" if enabled else "✗"
            print(f"  {status} {event.replace('_', ' ').title()}")
        print(f"Using {min(args.max_workers, len(events_to_process))} parallel workers")

        user_data_points = parse_points(args.input_file)
        
        # Create mapping of event types to their detection functions
        detection_functions = {
            'speeding': run_speeding_detection,
            'cornering': run_cornering_detection,
            'accel_decel': run_accel_decel_detection,
            'distracted': run_distracted_detection,
            'night_driving': run_night_driving_detection
        }

        # Run detection tasks in parallel
        start_time = time.time()
        results = {}
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as executor:
            # Submit all detection tasks
            future_to_event = {
                executor.submit(detection_functions[event_type], args.input_file): event_type
                for event_type in events_to_process
            }
            
            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_event):
                event_type = future_to_event[future]
                try:
                    event_type, result = future.result()
                    results[event_type] = result
                    print(f"✓ Completed {event_type} detection")
                except Exception as e:
                    print(f"✗ Error in {event_type} detection: {str(e)}")
                    results[event_type] = {'error': str(e)}

        total_processing_time = time.time() - start_time
        
        # Print results for each event type
        if enabled_events['speeding'] and 'speeding' in results and 'error' not in results['speeding']:
            # print_speeding_records(results['speeding']) ## Uncomment to print detailed speeding records for each geocode
            # print_speeding_service_metrics(results['speeding']['metrics'])
            print_grouped_speeding_events(results['speeding']['grouped_events'])
            if enabled_events['road_familiarity']:
                print_road_history_stats(results['speeding']['road_history_stats'])
            if enabled_events['road_types']:
                print_road_types_travelled(results['speeding']['road_types_travelled'])
            
        if enabled_events['cornering'] and 'cornering' in results and 'error' not in results['cornering']:
            print("\n" + "="*50)
            print("CORNERING EVENTS")
            print("="*50)
            print_cornering_events(results['cornering'])
            print(f"Total Cornering Events Detected: {len(results['cornering'])}")
            
        if (enabled_events['hard_braking'] or enabled_events['rapid_acceleration']) and 'accel_decel' in results and 'error' not in results['accel_decel']:
            print("\n" + "="*50)
            print("ACCELERATION/DECELERATION EVENTS")
            print("="*50)
            print_accel_decel_events(results['accel_decel'])
            print(f"Total Acceleration/Deceleration Events Detected: {len(results['accel_decel'])}")
        
        if enabled_events['distracted'] and 'distracted' in results and 'error' not in results['distracted']:
            print_distracted_events(results['distracted'])
            distracted_events = results['distracted']
        elif 'distracted' in results:
            print(f"Distracted Events: Error - {results['distracted']['error']}")
            distracted_events = []
        else:
            # Fallback for legacy runs
            distracted_events = []
            
        if enabled_events['night_driving'] and 'night_driving' in results and 'error' not in results['night_driving']:
            print_night_driving_events(results['night_driving'])
        elif 'night_driving' in results:
            print(f"Night Driving Events: Error - {results['night_driving']['error']}")    
        
        # Print comprehensive summary
        print("\n" + "="*50)
        print("COMPREHENSIVE SUMMARY")
        print("="*50)
        
        total_events = 0
        if enabled_events['speeding'] and 'speeding' in results and 'error' not in results['speeding']:
            speeding_event_count = len(results['speeding']['grouped_events'])
            total_events += speeding_event_count
            print(f"Speeding Events: {speeding_event_count}")
            print(f"Speeding Records for this Trip: {results['speeding']['metrics']['speeding_records']}")
            
            # Print road history summary
            road_history = results['speeding']['road_history_stats']
            if 'error' not in road_history:
                print(f"Road Segments - Total: {road_history['total_segments']}, Recently Driven: {road_history['segments_driven_recently']}, New: {road_history['segments_not_driven_recently']}")
        elif 'speeding' in results:
            print(f"Speeding Records: Error - {results['speeding']['error']}")
            
        if enabled_events['cornering'] and 'cornering' in results and 'error' not in results['cornering']:
            cornering_count = len(results['cornering'])
            total_events += cornering_count
            print(f"Cornering Events: {cornering_count}")
        elif 'cornering' in results:
            print(f"Cornering Events: Error - {results['cornering']['error']}")
            
        if (enabled_events['hard_braking'] or enabled_events['rapid_acceleration']) and 'accel_decel' in results and 'error' not in results['accel_decel']:
            accel_decel_count = len(results['accel_decel'])
            total_events += accel_decel_count
            print(f"Acceleration/Deceleration Events: {accel_decel_count}")
        elif 'accel_decel' in results:
            print(f"Acceleration/Deceleration Events: Error - {results['accel_decel']['error']}")
        
        if enabled_events['distracted'] and 'distracted' in results and 'error' not in results['distracted']:
            distracted_count = len(results['distracted'])
            total_events += distracted_count
            print(f"Distracted Events: {distracted_count}")
        elif 'distracted' in results:
            print(f"Distracted Events: Error - {results['distracted']['error']}")
        
        if 'night_driving' in results and 'error' not in results['night_driving']:
            night_driving_seconds = results['night_driving']['total_night_driving_seconds']
            print(f"Night Driving Time: {night_driving_seconds} seconds ({results['night_driving']['total_night_driving_hours']} hours)")
        elif 'night_driving' in results:
            print(f"Night Driving Events: Error - {results['night_driving']['error']}")

        print(f"\nTotal Events Detected: {total_events}")
        print(f"Total Parallel Processing Time: {total_processing_time:.2f} seconds")
        if enabled_events['speeding'] and 'speeding' in results and 'error' not in results['speeding']:
            print_speeding_service_metrics(results['speeding']['metrics'])
        
        # Generate trip summary
        print("\n" + "="*50)
        print("GENERATING TRIP SUMMARY")
        print("="*50)
        
        try:
            trip_summary = generate_trip_summary(results, user_data_points, config, enabled_events)
            duration_minutes, duration_seconds = convert_seconds(trip_summary.get('duration_seconds', 0))
            if 'error' in trip_summary:
                print(f"Error generating trip summary: {trip_summary['error']}")
            else:
                # Save trip summary to file
                output_file = save_trip_summary(trip_summary)
                print(f"Trip summary saved to: {output_file}")
                print(f"Trip Summary Stats:")
                print(f"  - Drive ID: {trip_summary['driveid']}")
                print(f"  - Total Distance: {trip_summary['distance_miles']:.2f} miles")
                print(f"  - Total Duration: {duration_minutes} minutes {duration_seconds} seconds")
                print(f"  - Total Events: {trip_summary['summary']['total_events']}")
                if enabled_events['speeding']:
                    print(f"  - Speeding Events: {trip_summary['summary']['speeding_events']}")
                if enabled_events['cornering']:
                    print(f"  - Cornering Events: {trip_summary['summary']['cornering_events']}")
                if enabled_events['hard_braking']:
                    print(f"  - Hard Braking Events: {trip_summary['summary']['hard_braking_events']}")
                if enabled_events['rapid_acceleration']:
                    print(f"  - Rapid Acceleration Events: {trip_summary['summary']['rapid_acceleration_events']}")
                if enabled_events['distracted']:
                    print(f"  - Distracted Events: {trip_summary['summary']['distracted_events']}")
                if enabled_events['night_driving']:
                    print(f"  - Night Driving: {trip_summary['summary']['night_driving_seconds']} seconds")

                # Send Results Dict to Scoring Algorithm for Overlap Detection + Scoring Flow
                calculate_driving_score(results, config, duration_seconds)

        except Exception as e:
            print(f"Error generating trip summary: {str(e)}")
            import traceback
            traceback.print_exc()
                    
        
    except FileNotFoundError:
        print(f"Error: Input file '{args.input_file}' not found")
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 