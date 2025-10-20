from datetime import datetime
import argparse
import concurrent.futures
import time
import os
from dotenv import load_dotenv
from detect_speeding_events import detect_speeding_records, convert_timestamp
from detect_cornering_events import detect_cornering_events_wrapper
from detect_accel_decel_events import detect_accel_decel_events_wrapper
from detect_distracted_events import detect_distracted_events
from detect_late_night_events import detect_late_night_events_wrapper
from decimal import Decimal

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
        'EXCESS_SPEED_THRESHOLD_MPH': int(os.environ.get('EXCESS_SPEED_THRESHOLD_MPH', '11')),
        'EXCESS_SPEED_DURATION_SECONDS': int(os.environ.get('EXCESS_SPEED_DURATION_SECONDS', '5')),

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
    }
    
    return config

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
        mapquest_speed_limit = (
            segment['mapquest_speed_limit']
            if isinstance(segment['mapquest_speed_limit'], (int, float)) and segment['mapquest_speed_limit'] > 0
            else 0
        )

        print(f"⏱️  Timestamp: {convert_timestamp(timestamp)}")
        print(f"📍 Location: {lat}, {lon}")
        print(f"📏  Distance from Driver to Nearest Road: {distance_meters} m")
        print(f"🚧 Road Segment ID: {segment['id'] if segment else 'None'}")
        print(f"🛣️  Road Segment: {segment['road_name']}")
        print(f"🚧  Road Type: {segment['road_type']}")
        print(f"🚦 OSM Speed Limit: {osm_speed_limit} mph")
        print(f"🚦 Mapillary Speed Limit: {mapillary_speed_limit} mph")
        print(f"🚦 MapQuest Speed Limit: {mapquest_speed_limit} mph")
        print(f"🚗 Traveling Speed: {traveling_speed} mph\n")
    
    # Print metrics
    metrics = results.get('metrics', {})
    print("======= SPEEDING RECORDS METRICS =======")
    print(f"Total Distance: {results['distance']:.2f} miles")
    print(f"Total Duration: {results['duration'][0]} minutes and {results['duration'][1]} seconds")
    print(f"# of User Geocodes: {metrics.get('user_geocodes', 0)}")
    print(f"# of travelled road segments: {metrics.get('travelled_segments', 0)}")
    print(f"# of Unique Segments: {metrics.get('unique_segments', 0)}")
    print(f"# of Segments where geocode > 5: {metrics.get('filtered_segments', 0)}")
    print(f"# of Segments Ignored (geocode < 5): {metrics.get('removed_segments', 0)}")
    print(f"# of Speeding Records (travelling > speed sign): {metrics.get('speeding_records', 0)}")
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
        start = event[0]
        end = event[-1]
        duration = end['timestamp'] - start['timestamp']
        print(f"Event {idx}:")
        print(f"  Start Time: {datetime.fromtimestamp(start['timestamp'])}")
        print(f"  End Time: {datetime.fromtimestamp(end['timestamp'])}")
        print(f"  Duration: {duration} seconds")
        print(f"  Road Type: {start['road_type']}")
        print(f"  Start Speed: {start['speed']} mph, Limit: {start['limit']} mph")
        print(f"  End Speed: {end['speed']} mph, Limit: {end['limit']} mph")
        print(f"  # Points: {len(event)}")
        print("  Details:")
        for point in event:
            print(f"    Time: {datetime.fromtimestamp(point['timestamp'])}, Speed: {point['speed']} mph, Limit: {point['limit']} mph, Road Type: {point['road_type']}, Location: ({point['lat']}, {point['long']})")
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
        print(f"  Start Time: {event['start_time_readable']}")
        print(f"  End Time: {event['end_time_readable']}")
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
        duration = end['timestamp_raw'] - start['timestamp_raw'] + 1
        print(f"Event: {event['type']}")
        print(f"  Location: ({start['lat']}, {start['lon']})")
        print(f"  Start Time: {start['timestamp_hr']}")
        print(f"  End Time: {end['timestamp_hr']}")
        print(f"  Duration: {duration} seconds")
        print(f"  Max Acceleration Magnitude: {event['max_accel']:.2f} mph/s\n")

def print_distracted_events(distracted_events):
    print("\n======= DISTRACTED EVENTS =======")
    print(f"Total Distracted Events: {len(distracted_events)}")
    for i, event in enumerate(distracted_events, 1):
        print(f"Event {i}: Start idx {event['start_idx']}, End idx {event['end_idx']}, Start time {event['start_time']}, End time {event['end_time']}, Length {event['length']}")

def print_late_night_events(late_night_results):
    """
    Print late night driving events in a formatted way.
    
    Args:
        late_night_results (dict): Results from late night events detection
    """
    if 'error' in late_night_results:
        print(f"Error in late night detection: {late_night_results['error']}")
        return
        
    print("\n" + "="*50)
    print("LATE NIGHT DRIVING EVENTS (12 AM - 4 AM)")
    print("="*50)
    print(f"Timezone: {late_night_results['timezone_used']}")
    print(f"Total Late Night Driving Time: {late_night_results['total_late_night_seconds']} seconds")
    print(f"Total Late Night Driving Time: {late_night_results['total_late_night_minutes']} minutes")
    print(f"Total Late Night Driving Time: {late_night_results['total_late_night_hours']} hours")
    print(f"Total Points in Dataset: {late_night_results['total_points']}")
    print(f"Points During Late Night Hours: {late_night_results['late_night_points']}")
    print(f"Percentage of Points During Late Night: {late_night_results['late_night_percentage']}%")

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
        print(f"New/Expired Segment IDs: {road_history_stats['segments_not_driven_recently_ids']}")
    elif 'segments_not_driven_recently_ids' in road_history_stats:
        print(f"New/Expired Segment IDs: {len(road_history_stats['segments_not_driven_recently_ids'])} segments (too many to display)")

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
    Wrapper function for acceleration/deceleration detection to be used with ThreadPoolExecutor.
    
    Args:
        input_file (str): Path to the input file
        
    Returns:
        tuple: (event_type, results)
    """
    try:
        config = get_config()
        results = detect_accel_decel_events_wrapper(input_file, config=config)
        return ('accel_decel', results)
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

def run_late_night_detection(input_file):
    """
    Wrapper function for late night detection to be used with ThreadPoolExecutor.
    
    Args:
        input_file (str): Path to the input file
        
    Returns:
        tuple: (event_type, results)
    """
    try:
        config = get_config()
        results = detect_late_night_events_wrapper(input_file, config=config)
        return ('late_night', results)
    except Exception as e:
        return ('late_night', {'error': str(e)})

def event_time_overlap(event1, event2):
    """
    Returns True if event1 and event2 time windows overlap.
    event1/event2: dicts with 'start_time' and 'end_time' (unix timestamps)
    """
    return not (event1['end_time'] < event2['start_time'] or event2['end_time'] < event1['start_time'])

def distracted_event_overlap_analysis(distracted_events, accel_events, speeding_events, cornering_events):
    """
    For each distracted event, check for overlap with accel/decel, speeding, and cornering events.
    Print details and count overlaps.
    """
    overlap_counts = {'accel_decel': 0, 'speeding': 0, 'cornering': 0}
    print("\n======= DISTRACTED EVENT OVERLAP ANALYSIS =======")
    for d_event in distracted_events:
        # Accel/Decel
        for a_event in accel_events:
            a_start = a_event['start']['timestamp_raw']
            a_end = a_event['end']['timestamp_raw']
            accel_event = {'start_time': a_start, 'end_time': a_end}
            if event_time_overlap(d_event, accel_event):
                overlap_counts['accel_decel'] += 1
                print(f"Distracted event ({d_event['start_time']} - {d_event['end_time']}, len={d_event['length']}) OVERLAPS with Accel/Decel event ({a_start} - {a_end}, type={a_event['type']})")
        # Speeding
        for s_event in speeding_events:
            s_time = int(s_event['PutRequest']['Item']['timestamp#user_id'].split('#')[0])
            # Speeding events are point events, treat as instant
            if d_event['start_time'] <= s_time <= d_event['end_time']:
                overlap_counts['speeding'] += 1
                print(f"Distracted event ({d_event['start_time']} - {d_event['end_time']}, len={d_event['length']}) OVERLAPS with Speeding event ({s_time})")
        # Cornering
        for c_event in cornering_events:
            c_start = c_event['start_time_unix']
            c_end = c_event['end_time_unix']
            corner_event = {'start_time': c_start, 'end_time': c_end}
            if event_time_overlap(d_event, corner_event):
                overlap_counts['cornering'] += 1
                print(f"Distracted event ({d_event['start_time']} - {d_event['end_time']}, len={d_event['length']}) OVERLAPS with Cornering event ({c_start} - {c_end}, type={c_event['event_type']})")
    print("\n======= DISTRACTED EVENT OVERLAP COUNTS =======")
    print(f"Accel/Decel Overlaps: {overlap_counts['accel_decel']}")
    print(f"Speeding Overlaps: {overlap_counts['speeding']}")
    print(f"Cornering Overlaps: {overlap_counts['cornering']}")
    return overlap_counts

def convert_decimals(obj):
    if isinstance(obj, list):
        return [convert_decimals(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        return float(obj)
    else:
        return obj

def main():
    """
    Main entry point for detecting all driving events.
    Processes command line arguments and calls the appropriate detection functions in parallel.
    """
    parser = argparse.ArgumentParser(description='Detect all driving events from input file')
    parser.add_argument('input_file', help='Path to the input file containing driving data')
    parser.add_argument('--events', nargs='+', choices=['speeding', 'cornering', 'accel_decel', 'distracted', 'late_night', 'all'], 
                       default=['all'], help='Types of events to detect')
    parser.add_argument('--max-workers', type=int, default=3, 
                       help='Maximum number of parallel workers (default: 3)')
    parser.add_argument('--min-distracted', type=int, default=3, help='Minimum consecutive distracted points (default: 3)')
    args = parser.parse_args()

    try:
        # Determine which events to process
        events_to_process = []
        if 'all' in args.events:
            events_to_process = ['speeding', 'cornering', 'accel_decel', 'distracted', 'late_night']
        else:
            events_to_process = args.events

        print(f"Processing events: {', '.join(events_to_process)}")
        print(f"Using {min(args.max_workers, len(events_to_process))} parallel workers")
        
        # Create mapping of event types to their detection functions
        config = get_config()
        detection_functions = {
            'speeding': run_speeding_detection,
            'cornering': run_cornering_detection,
            'accel_decel': run_accel_decel_detection,
            'distracted': run_distracted_detection,
            'late_night': run_late_night_detection
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
        if 'speeding' in results and 'error' not in results['speeding']:
            print_speeding_records(results['speeding'])
            print_grouped_speeding_events(results['speeding']['grouped_events'])
            print_road_history_stats(results['speeding']['road_history_stats'])
            
        if 'cornering' in results and 'error' not in results['cornering']:
            print("\n" + "="*50)
            print("CORNERING EVENTS")
            print("="*50)
            print_cornering_events(results['cornering'])
            print(f"Total Cornering Events Detected: {len(results['cornering'])}")
            
        if 'accel_decel' in results and 'error' not in results['accel_decel']:
            print("\n" + "="*50)
            print("ACCELERATION/DECELERATION EVENTS")
            print("="*50)
            print_accel_decel_events(results['accel_decel'])
            print(f"Total Acceleration/Deceleration Events Detected: {len(results['accel_decel'])}")
        
        if 'distracted' in results and 'error' not in results['distracted']:
            print_distracted_events(results['distracted'])
            distracted_events = results['distracted']
        elif 'distracted' in results:
            print(f"Distracted Events: Error - {results['distracted']['error']}")
            distracted_events = []
        else:
            # Fallback for legacy runs
            distracted_events = []
            
        if 'late_night' in results and 'error' not in results['late_night']:
            print_late_night_events(results['late_night'])
        elif 'late_night' in results:
            print(f"Late Night Events: Error - {results['late_night']['error']}")

        # Overlap analysis
        # accel_events = results.get('accel_decel', [])
        # speeding_records = results.get('speeding', {}).get('grouped_events', [])
        # cornering_events = results.get('cornering', [])
       
        
        # Print comprehensive summary
        print("\n" + "="*50)
        print("COMPREHENSIVE SUMMARY")
        print("="*50)
        
        total_events = 0
        if 'speeding' in results and 'error' not in results['speeding']:
            speeding_event_count = len(results['speeding']['grouped_events'])
            total_events += speeding_event_count
            print(f"Speeding Events: {speeding_event_count}")
            print(f"Speeding Records for this Trip: {results['speeding']['metrics']['speeding_records']}")
            print(f"Total Distance: {results['speeding']['distance']:.2f} miles")
            print(f"Total Duration: {results['speeding']['duration'][0]} minutes and {results['speeding']['duration'][1]} seconds")
            
            # Print road history summary
            road_history = results['speeding']['road_history_stats']
            if 'error' not in road_history:
                print(f"Road Segments - Total: {road_history['total_segments']}, Recently Driven: {road_history['segments_driven_recently']}, New/Expired: {road_history['segments_not_driven_recently']}")
        elif 'speeding' in results:
            print(f"Speeding Records: Error - {results['speeding']['error']}")
            
        if 'cornering' in results and 'error' not in results['cornering']:
            cornering_count = len(results['cornering'])
            total_events += cornering_count
            print(f"Cornering Events: {cornering_count}")
        elif 'cornering' in results:
            print(f"Cornering Events: Error - {results['cornering']['error']}")
            
        if 'accel_decel' in results and 'error' not in results['accel_decel']:
            accel_decel_count = len(results['accel_decel'])
            total_events += accel_decel_count
            print(f"Acceleration/Deceleration Events: {accel_decel_count}")
        elif 'accel_decel' in results:
            print(f"Acceleration/Deceleration Events: Error - {results['accel_decel']['error']}")
        
        print(f"Distracted Driving Events: {len(distracted_events)}")
        # Add Distracted Events
        total_events += (len(distracted_events))
        
        if 'late_night' in results and 'error' not in results['late_night']:
            late_night_seconds = results['late_night']['total_late_night_seconds']
            print(f"Late Night Driving Time: {late_night_seconds} seconds ({results['late_night']['total_late_night_hours']} hours)")
        elif 'late_night' in results:
            print(f"Late Night Events: Error - {results['late_night']['error']}")

        print(f"\nTotal Events Detected: {total_events}")
        print(f"Total Parallel Processing Time: {total_processing_time:.2f} seconds")
        
        # # Show performance improvement if speeding events were processed
        # if 'speeding' in results and 'error' not in results['speeding']:
        #     speeding_alone_time = results['speeding']['metrics']['timings']['total']
        #     if len(events_to_process) > 1:
        #         improvement = ((speeding_alone_time - total_processing_time) / speeding_alone_time) * 100
        #         print(f"Performance improvement: {improvement:.1f}% faster than sequential processing")
        
    except FileNotFoundError:
        print(f"Error: Input file '{args.input_file}' not found")
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 