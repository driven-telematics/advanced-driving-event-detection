from datetime import datetime, timezone
import requests
import time
from geopy.distance import geodesic
from shapely.geometry import Point, LineString
from shapely.ops import nearest_points
import boto3
from collections import defaultdict
from helper_functions import extract_float, normalize_decimal
from decimal import Decimal, ROUND_UP, ROUND_DOWN

# Initialize the DynamoDB client
session = boto3.Session(profile_name='local-dynamo-db-access')
dynamodb = session.resource('dynamodb')

# Function to batch fetch road segment data from DynamoDB
def batch_get_items(keys, config):
    results = {}
    batch_size = config.get('BATCH_SIZE', 20)
    for batch in (keys[i : i + batch_size] for i in range(0, len(keys), batch_size)):
        response = dynamodb.batch_get_item(
            RequestItems={
                config.get('DYNAMODB_ROAD_SEGMENT_TABLE', 'drivenDB_road_segment_info'): {
                    'Keys': [{'road_segment_id': segment_id} for segment_id in batch]
                }
            }
        )
        items = response.get('Responses', {}).get(config.get('DYNAMODB_ROAD_SEGMENT_TABLE', 'drivenDB_road_segment_info'), [])
        results.update({item['road_segment_id']: item for item in items})
    return results

def batch_write_all(table_name, items, config):
    batch_size = config.get('DB_BATCH_SIZE', 25)
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]  
        request_items = {table_name: batch}

        dynamodb.batch_write_item(RequestItems=request_items)

def batch_get_user_segment_history(segment_ids, user_id, config):
    """
    Batch fetch user's road segment history from DynamoDB for the given segment IDs.
    With TTL, we only need to check if records exist - DynamoDB automatically removes expired ones.
    
    Args:
        segment_ids (list): List of road segment IDs to check
        user_id (str): User ID to query for
        config (dict): Configuration dictionary
        
    Returns:
        dict: Dictionary mapping segment_id to history record
    """
    results = {}
    batch_size = config.get('BATCH_SIZE', 20)
    table_name = config.get('DYNAMODB_USER_ROAD_HISTORY_TABLE', 'user_road_segment_history')
    
    # Process segments in batches
    for batch in (segment_ids[i : i + batch_size] for i in range(0, len(segment_ids), batch_size)):
        # Create batch query items - much simpler with TTL
        batch_items = []
        for segment_id in batch:
            batch_items.append({
                'user_id': user_id,
                'road_segment_id': segment_id
            })
        
        try:
            response = dynamodb.batch_get_item(
                RequestItems={
                    table_name: {
                        'Keys': batch_items
                    }
                }
            )
            
            items = response.get('Responses', {}).get(table_name, [])
            # Convert list to dict for easier lookup
            for item in items:
                segment_id = item['road_segment_id']
                results[segment_id] = item
                
        except Exception as e:
            print(f"Error fetching user segment history: {e}")
            continue
    
    return results

def create_update_user_segment_record(segment_id, user_id, road_segment_data, existing_record=None):
    """
    Create a DynamoDB update record for an existing road segment history entry.
    Updates the TTL to extend it by 21 days from the last driven time.
    
    Args:
        segment_id (str): Road segment ID
        user_id (str): User ID
        road_segment_data (dict): Road segment information from travelled_segments
        existing_record (dict): Existing record data (optional)
        
    Returns:
        dict: DynamoDB PutRequest item
    """
    current_timestamp = int(time.time())
    current_date = datetime.fromtimestamp(current_timestamp, tz=timezone.utc).strftime('%Y-%m-%d')
    
    # Calculate TTL: current time + 21 days
    # ttl_timestamp = current_timestamp + (21 * 24 * 60 * 60)
    ttl_timestamp = current_timestamp + (24 * 60 * 60) # Testing - set to 1 day TTL

    # If updating existing record, preserve first_driven_date and increment drive_count
    if existing_record:
        first_driven_date = existing_record.get('first_driven_date', current_date)
        drive_count = existing_record.get('drive_count', 1) + 1
        created_at = existing_record.get('created_at', current_timestamp)
    else:
        first_driven_date = current_date
        drive_count = 1
        created_at = current_timestamp
    
    update_record = {
        "PutRequest": {
            "Item": {
                "user_id": user_id,
                "road_segment_id": segment_id,
                "last_driven_date": current_date,
                "first_driven_date": first_driven_date,
                "drive_count": drive_count,
                "last_driven_timestamp": current_timestamp,
                "created_at": created_at,
                "updated_at": current_timestamp,
                "road_name": road_segment_data.get('road_name', 'Unnamed Road'),
                "road_type": road_segment_data.get('road_type', 'Unknown'),
                "record_expiration": ttl_timestamp  # TTL: 21 days from now
            }
        }
    }
    
    return update_record

def create_new_user_segment_record(segment_id, user_id, road_segment_data):
    """
    Create a new DynamoDB record for a road segment.
    
    Args:
        segment_id (str): Road segment ID
        user_id (str): User ID
        road_segment_data (dict): Road segment information from travelled_segments
        
    Returns:
        dict: DynamoDB PutRequest item
    """
    current_timestamp = int(time.time())
    current_date = datetime.fromtimestamp(current_timestamp, tz=timezone.utc).strftime('%Y-%m-%d')
    
    # ttl_timestamp = current_timestamp + (21 * 24 * 60 * 60)
    ttl_timestamp = current_timestamp + (24 * 60 * 60) # Testing - set to 1 day TTL
    
    new_record = {
        "PutRequest": {
            "Item": {
                "user_id": user_id,
                "road_segment_id": segment_id,
                "last_driven_date": current_date,
                "first_driven_date": current_date,
                "drive_count": 1,
                "last_driven_timestamp": current_timestamp,
                "created_at": current_timestamp,
                "updated_at": current_timestamp,
                "road_name": road_segment_data.get('road_name', 'Unnamed Road'),
                "road_type": road_segment_data.get('road_type', 'Unknown'),
                "record_expiration": ttl_timestamp 
            }
        }
    }
    
    return new_record

def check_and_update_user_segment_history(travelled_segments, user_id, config):
    """
    Check which road segments the user has driven in the past 21 days
    and update the history accordingly. Uses DynamoDB TTL for automatic cleanup.
    
    Args:
        travelled_segments (dict): Dictionary of road segments from current trip
        user_id (str): User ID
        config (dict): Configuration dictionary
        
    Returns:
        dict: Statistics about segments driven vs not driven recently
    """
    # Get unique segment IDs from current trip
    current_trip_segments = list(travelled_segments.keys())
    
    if not current_trip_segments:
        return {
            'segments_driven_recently': 0,
            'segments_not_driven_recently': 0,
            'total_segments': 0,
            'error': 'No segments to process'
        }
    
    # Batch query existing history for these segments
    # With TTL, any returned records are automatically within the 21-day window
    existing_history = batch_get_user_segment_history(current_trip_segments, user_id, config)
    
    # Track segments driven vs not driven in past 21 days
    segments_driven_recently = []
    segments_not_driven_recently = []
    records_to_write = []
    
    # Process each segment
    for segment_id in current_trip_segments:
        if segment_id in existing_history:
            # If record exists, it's within 21 days (TTL ensures this)
            segments_driven_recently.append(segment_id)
            # Update existing record with new timestamp and extend TTL
            records_to_write.append(create_update_user_segment_record(segment_id, user_id, travelled_segments[segment_id], existing_history[segment_id]))
        else:
            # No record exists or it was expired by TTL
            segments_not_driven_recently.append(segment_id)
            # Create new record
            records_to_write.append(create_new_user_segment_record(segment_id, user_id, travelled_segments[segment_id]))
    
    # Batch write all updates
    if records_to_write:
        table_name = config.get('DYNAMODB_USER_ROAD_HISTORY_TABLE', 'user_road_segment_history')
        batch_write_all(table_name, records_to_write, config)
    
    return {
        'segments_driven_recently': len(segments_driven_recently),
        'segments_not_driven_recently': len(segments_not_driven_recently),
        'total_segments': len(current_trip_segments),
        'segments_driven_recently_ids': segments_driven_recently,
        'segments_not_driven_recently_ids': segments_not_driven_recently
    }

def determine_road_types_travelled(travelled_segments, config):
    """
    Determine how many times each road classification type was travelled.

    Args:
        travelled_segments (dict): A dictionary of segments, where each value contains at least a 'road_type' key.
        config (dict): Configuration containing 'ROAD_CLASSIFICATIONS' (list of valid road types).

    Returns:
        dict: A dictionary where each road classification type is a key,
              with counts of how many times that type was travelled,
              plus a total under 'road_types_travelled_count'.
    """
    # Get the list of valid road classification types
    road_classifications = config.get('ROAD_CLASSIFICATIONS', [])
    road_classifications = [road_type.lower() for road_type in road_classifications]

    # Initialize counts for each road classification
    road_types_travelled = {road_type: 0 for road_type in road_classifications}
    road_types_travelled['road_types_travelled_count'] = 0

    # Iterate over all segments
    for segment in travelled_segments.values():
        road_type = segment.get('road_type')

        # Only count recognized classifications
        if road_type in road_types_travelled:
            road_types_travelled[road_type] += 1

    # Compute the total count
    total_count = sum(
        count for key, count in road_types_travelled.items()
        if key != 'road_types_travelled_count'
    )
    road_types_travelled['road_types_travelled_count'] = total_count

    return road_types_travelled

def count_segment_occurrences(geocode_to_segment):
    segment_count = defaultdict(int)

    # Count occurrences of each segment_id
    for value in geocode_to_segment.values():
        segment_id = value["segment_id"]
        segment_count[segment_id] += 1

    # Filter out segments with count < n number of occurences
    filtered_segment_count = {seg_id: count for seg_id, count in segment_count.items() if count >= 5}
    removed_segment_count = {seg_id: count for seg_id, count in segment_count.items() if count < 5}

    return filtered_segment_count, removed_segment_count, len(segment_count)

def convert_to_lat_lon(coords):
    return [(entry['lat'], entry['lon']) for entry in coords]

def get_bounding_box(points):
    latitudes = [p[0] for p in points]
    longitudes = [p[1] for p in points]
    return min(latitudes), max(latitudes), min(longitudes), max(longitudes)

# Function to query Overpass API for road segments within bounding box
def get_road_segments(lat_min, lon_min, lat_max, lon_max, config):
    query = f"""
    [out:json];
    way({lat_min},{lon_min},{lat_max},{lon_max})[highway];
    out geom;
    """
    response = requests.get(config.get('DRIVEN_OVERPASS_URL', ''), params={"data": query})
    
    if response.status_code == 200:
        if not response.text.strip():  # Check if response is empty
            print("Error: Received empty response from Overpass API")
            return []
        try:
            return response.json().get("elements", [])
        except requests.exceptions.JSONDecodeError:
            print(f"Error decoding JSON: {response.text}")
            return []
    else:
        print(f"Error fetching data: {response.status_code}, {response.text}")
        return []

# Function to parse speed limits from Mapillary
def parse_mapillary_speed_limit(object_value):
    parts = object_value.split("-")
    if len(parts) < 4:
        return None
    try:
        return float(parts[-3])
    except ValueError:
        return None

# Function to get speed limits from Mapillary within bounding box
def get_mapillary_speed_limits(lat_min, lon_min, lat_max, lon_max, config):
    bbox = f"{lon_min},{lat_min},{lon_max},{lat_max}"
    url = f"https://graph.mapillary.com/map_features?access_token={config.get('MAPILLARY_ACCESS_TOKEN', '')}&fields=id,object_value,geometry&bbox={bbox}&layers=trafficsigns"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            return [
                item for item in response.json().get("data", [])
                if "regulatory--maximum-speed-limit" in item.get("object_value", "")
            ]
        else:
            print(f"Mapillary API Error: {response.status_code}, {response.text}")
            return []
    except requests.RequestException as e:
        print(f"Mapillary API Request Failed: {e}")
        return []

# Helper function to find distance between speed sign and road segment
def calculate_distance_to_road_segment(sign_coords, road_coords):
    """
    Calculate the minimum perpendicular distance from a speed limit sign to a road segment.

    :param sign_coords: Tuple (latitude, longitude) of the speed sign.
    :param road_coords: List of tuples [(lat1, lon1), (lat2, lon2), ...] representing the road segment.
    :return: Minimum distance in meters.
    """
    sign_location = Point(sign_coords[1], sign_coords[0])  # Convert to (lon, lat) for Shapely
    min_distance = float('inf')

    for i in range(len(road_coords) - 1):
        segment = LineString([(road_coords[i][1], road_coords[i][0]), (road_coords[i+1][1], road_coords[i+1][0])])

        nearest_point = nearest_points(segment, sign_location)[0]  
        distance_meters = geodesic((sign_coords[0], sign_coords[1]), (nearest_point.y, nearest_point.x)).meters

        min_distance = min(min_distance, distance_meters)

    return min_distance

def map_speed_sign_to_nearest_road(nearest_road, maplillary_speed_signs):
    minlat, maxlat = nearest_road['bounds']['minlat'], nearest_road['bounds']['maxlat']
    minlon, maxlon = nearest_road['bounds']['minlon'], nearest_road['bounds']['maxlon']
    road_coords = [(point["lat"], point["lon"]) for point in nearest_road["geometry"]]
    # Ensure road segment has a "speed_signs" field
    nearest_road.setdefault("mapillary_speed_signs", [])

    for sign in maplillary_speed_signs:
        sign_coords = (sign["geometry"]["coordinates"][1], sign["geometry"]["coordinates"][0])  # (lat, lon)
        distance = calculate_distance_to_road_segment(sign_coords, road_coords)

        if distance < 10 and minlat <= sign_coords[0] <= maxlat and minlon <= sign_coords[1] <= maxlon:  # Assign sign if within 10 meters
        # if distance < 20:  # Assign sign if within 10 meters
            nearest_road["mapillary_speed_signs"].append(
                {
                    "sign_id": sign["id"],
                    "object_value": sign["object_value"],
                    "speed_limit": parse_mapillary_speed_limit(sign["object_value"]), 
                    "sign_coords": sign_coords,
                    "distance": distance
                }
            )

    return nearest_road

# Helper function to find distance between user and road segment
def calculate_distance_user_to_road_segment(user_coords, road_coords):
    """
    Calculate the minimum perpendicular distance from a speed limit sign to a road segment.

    :param sign_coords: Tuple (latitude, longitude) of the speed sign.
    :param road_coords: List of tuples [(lat1, lon1), (lat2, lon2), ...] representing the road segment.
    :return: Minimum distance in meters.
    """
    sign_location = Point(user_coords[1], user_coords[0])  # Convert to (lon, lat) for Shapely
    min_distance = float('inf')

    for i in range(len(road_coords) - 1):
        segment = LineString([(road_coords[i][1], road_coords[i][0]), (road_coords[i+1][1], road_coords[i+1][0])])

        # Convert degrees to meters using geopy
        nearest_point = nearest_points(segment, sign_location)[0]  
        distance_meters = geodesic((user_coords[0], user_coords[1]), (nearest_point.y, nearest_point.x)).meters

        min_distance = min(min_distance, distance_meters)

    return min_distance

"""
Previous method has params: updated_road_segments
"""
def find_nearest_road(user_coords, road_segments):
    closest_road = None
    min_distance = float("inf")

    for road in road_segments:
        road_coords = road_coords = [(point["lat"], point["lon"]) for point in road["geometry"]]
        distance = calculate_distance_user_to_road_segment(user_coords, road_coords)

        if distance < min_distance:
            min_distance = distance
            speed_limit = road["tags"].get("maxspeed", "Unknown")
            parsed_speed_limit = extract_float(speed_limit) if "mph" in speed_limit else speed_limit
            
            closest_road = {
                "id": road.get("id"),
                "road_name": road["tags"].get("name", "Unnamed Road"),
                "road_type": road["tags"].get("highway", "Unknown"),
                "osm_speed_limit": parsed_speed_limit,
                "mapillary_speed_limit": -1.0,
                "distance_meters": round(distance, 2),
                "geometry": road["geometry"],
                "bounds": road["bounds"],
            }
            
    return closest_road
                    

def get_mapquest_speed_limit(coord, config):
    url = f"https://www.mapquestapi.com/geocoding/v1/reverse"
    params = {
        "key": config.get('MAPQUEST_API_KEY', ''),
        "location": f"{coord[0]},{coord[1]}",
        "includeRoadMetadata": "true"
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        try:
            road_metadata = data["results"][0]["locations"][0].get("roadMetadata", {})
            speed_limit = "Unknown"
            if road_metadata:
                speed_limit = road_metadata.get("speedLimit", "Unknown")

                if speed_limit:
                 speed_limit = float(speed_limit)

            return speed_limit
        except (IndexError, KeyError):
            return "Unexpected response format from API."
    else:
        return f"Error: {response.status_code} - {response.text}"                    
                

def convert_points_for_speeding_events(geocode_to_segment, travelled_segments):
    """
    Convert points and segment info to dicts with lat, long, speed, limit, road_type, timestamp for event detection.
    """
    result = []
    for (lat, lon, distracted, traveling_speed, timestamp), segment_data in geocode_to_segment.items():
        segment_id = segment_data['segment_id']
        segment = travelled_segments[segment_id]
        avg_segment_traveling_speed = segment.get('avg_traveling_speed', 0)
        avg_speed_deviation = segment.get('avg_speed_deviation', 0)
        driver_speed_deviation = normalize_decimal(traveling_speed) - normalize_decimal(avg_segment_traveling_speed) + normalize_decimal(avg_speed_deviation)

        result.append({
            'lat': lat,
            'long': lon,
            'distracted': bool(distracted),
            'speed': normalize_decimal(traveling_speed),
            'segment_id': segment_id,
            'avg_segment_traveling_speed': avg_segment_traveling_speed,
            'avg_speed_deviation': avg_speed_deviation,
            'driver_speed_deviation': driver_speed_deviation,
            'road_type': segment['road_type'],
            'timestamp': timestamp
        })

    return result

def driven_defined_speeding_events(points, config=None):
    # Use config for threshold and duration if provided, else default to 10 mph and 5 seconds
    threshold = normalize_decimal(10.0)
    duration = 5
    if config is not None:
        threshold = normalize_decimal(config.get('EXCESS_SPEED_THRESHOLD_MPH', 10.0))
        duration = int(config.get('EXCESS_SPEED_DURATION_SECONDS', 5))

    speeding_events = []
    current_event = []
    start_time = None

    for point in points:
        driver_speed_deviation = point['driver_speed_deviation']
        print("Driver Speed Deviation: ", driver_speed_deviation)
        print("Point Data: ", point, "\n")

        if driver_speed_deviation >= threshold and point['avg_segment_traveling_speed'] > 0 and point['speed'] > driver_speed_deviation:
            if not current_event:
                start_time = point['timestamp']
            current_event.append(point)
        else:
            # Check if the event meets the minimum duration requirement
            if current_event and start_time is not None:
                end_time = current_event[-1]['timestamp']
                if (end_time - start_time) >= duration:
                    speeding_events.append({
                        'event_id': f'speeding_{len(speeding_events)}',
                        'start_time': start_time,
                        'end_time': end_time,
                        'duration': end_time - start_time,
                        'start_speed': current_event[0]['speed'],
                        'end_speed': current_event[-1]['speed'],
                        'driver_speed_deviation_start': current_event[0]['driver_speed_deviation'],
                        'driver_speed_deviation_end': current_event[-1]['driver_speed_deviation'],
                        'road_type': current_event[0]['road_type'],
                        'points': current_event.copy()
                    })
            # Reset for the next event
            current_event = []
            start_time = None
    # Handle case where final event reaches the end of the dataset
    if current_event and start_time is not None:
        end_time = current_event[-1]['timestamp']
        if (end_time - start_time) >= duration:
            speeding_events.append({
                'event_id': f'speeding_{len(speeding_events)}',
                'start_time': start_time,
                'end_time': end_time,
                'duration': end_time - start_time,
                'start_speed': current_event[0]['speed'],
                'end_speed': current_event[-1]['speed'],
                'driver_speed_deviation_start': current_event[0]['driver_speed_deviation'],
                'driver_speed_deviation_end': current_event[-1]['driver_speed_deviation'],
                'road_type': current_event[0]['road_type'],
                'points': current_event
            })
    return speeding_events

# Function to process the input file and detect speeding events
def process_data_file(input_file, config):
    algo_start_time = time.time()
    reading_file_start_time = time.time()

    with open(input_file, "r") as file:
        data = file.read().strip().split("|")
        points = [(float(p[0]), float(p[1]), int(p[2]), float(p[3]), int(p[4])) for p in (row.split(",") for row in data)]
        latitudes = [float(p[0]) for p in points]
        longitudes = [float(p[1]) for p in points]
        session_lat_min, session_lat_max = min(latitudes), max(latitudes)
        session_lon_min, session_lon_max = min(longitudes), max(longitudes)
    
    reading_file_end_time = time.time()
    elapsed_reading_file_time = reading_file_end_time - reading_file_start_time
    
    total_points = len(points)
    batch_start = 0
    osm_api_call = 0
    batch_size = config.get('BATCH_SIZE', 20)

    mapillary_api_call_start_time = time.time()
    # DEV NOTE: Will need to optimize + build for scale when using Mapillary service
    mapillary_speed_signs = get_mapillary_speed_limits(session_lat_min, session_lon_min, session_lat_max, session_lon_max, config)

    mapillary_api_call_end_time = time.time()
    elapsed_mapillary_api_call_time = mapillary_api_call_end_time - mapillary_api_call_start_time


    # Track unique travelled segments across all batches
    determine_travelled_segments_start_time = time.time()

    travelled_segments = {}
    geocode_to_segment = {}
    # Track sum + count of speeds per segment
    segment_traveling_speed_stats = {} 

    while batch_start < total_points:
        batch_end = min(batch_start + batch_size, total_points)
        batch = points[batch_start:batch_end]
        lat_min, lat_max, lon_min, lon_max = get_bounding_box(batch)

        road_segments = get_road_segments(lat_min, lon_min, lat_max, lon_max, config)
        osm_api_call += 1
        
        for lat, lon, distracted, traveling_speed, timestamp in batch:
            user_coords = (lat, lon)
            nearest_road = find_nearest_road(user_coords, road_segments)
            
            if nearest_road:
                segment_id = str(nearest_road['id'])

                if segment_id not in travelled_segments:
                    travelled_segments[segment_id] = nearest_road

                # Track the mapping of driving data point to road segment
                geocode_to_segment[(lat, lon, distracted, traveling_speed, timestamp)] = {
                    "segment_id": segment_id,
                    "distance_meters": nearest_road['distance_meters']
                }

                if segment_id not in segment_traveling_speed_stats:
                    segment_traveling_speed_stats[segment_id] = {"traveling_speed_sum": 0.0, "count": 0}

                segment_traveling_speed_stats[segment_id]["traveling_speed_sum"] += traveling_speed
                segment_traveling_speed_stats[segment_id]["count"] += 1

        batch_start += batch_size

    # Calculate average driver traveling speed for each road segment
    for segment_id, stats in segment_traveling_speed_stats.items():
        traveling_speed_sum = stats["traveling_speed_sum"]
        count = stats["count"]
        avg_traveling_speed = traveling_speed_sum / count if count > 0 else 0

        travelled_segments[segment_id]["driver_avg_traveling_speed"] = normalize_decimal(avg_traveling_speed)

    filtered_geocode_to_segment, removed_segments, unique_segments_count = count_segment_occurrences(geocode_to_segment)

    # Check and update road segment history for 21-day tracking
    road_history_start_time = time.time()

    user_id = str(config.get('USER_ID', "31399")) 
    history_stats = check_and_update_user_segment_history(travelled_segments, user_id, config)
    
    road_history_end_time = time.time()
    elapsed_road_history_time = road_history_end_time - road_history_start_time

    road_types_travelled = determine_road_types_travelled(travelled_segments, config)
    determine_travelled_segments_end_time = time.time()
    elapsed_determine_travelled_segments = determine_travelled_segments_end_time - determine_travelled_segments_start_time

    resolve_speed_limits_start_time = time.time()
    segment_ids = list(travelled_segments.keys())
    db_existing_segments = batch_get_items(segment_ids, config)
    db_items_to_write = []

    mapquest_api_counter = 0
    segments_with_unknown_osm_speeds = 0
    updated_at_timestamp = int(time.time())

    # Process all traveled road segments
    for segment_id, road in travelled_segments.items():
        # Need to use average traveling speed because individual points have varying speeds
        driver_avg_traveling_speed = normalize_decimal(road['driver_avg_traveling_speed'])
        # Only process segments that passed geocode filtering
        if segment_id in filtered_geocode_to_segment:
            # ---------------------------------------------
            # CASE 1: EXISTING SEGMENT IN DATABASE
            # ---------------------------------------------
            if segment_id in db_existing_segments:
                # Use existing record data
                item = db_existing_segments[segment_id]

                db_item_avg_traveling_speed = normalize_decimal(item.get('avg_traveling_speed', normalize_decimal(0)))
                db_item_avg_speed_deviation = normalize_decimal(item.get('avg_speed_deviation', normalize_decimal(0)))
                db_item_original_drive_count = int(item.get('drive_count', 0))

                current_avg_driver_speed_deviation = driver_avg_traveling_speed - db_item_avg_traveling_speed + db_item_avg_speed_deviation

                updated_drive_count = db_item_original_drive_count + 1

                # Update running averages
                updated_avg_traveling_speed = ((db_item_avg_traveling_speed * db_item_original_drive_count) + driver_avg_traveling_speed) / updated_drive_count
                updated_avg_speed_deviation = ((db_item_avg_speed_deviation * db_item_original_drive_count) + current_avg_driver_speed_deviation) / updated_drive_count

                # Store updated values back into road object
                road['avg_traveling_speed'] = updated_avg_traveling_speed
                road['avg_speed_deviation'] = updated_avg_speed_deviation

                # Prepare updated DynamoDB item
                updated_item = {
                    "road_segment_id": segment_id,
                    "osm_road_name": item.get("osm_road_name"),
                    "osm_road_type": item.get("osm_road_type"),
                    "avg_traveling_speed": normalize_decimal(str(updated_avg_traveling_speed)),
                    "avg_speed_deviation": normalize_decimal(str(updated_avg_speed_deviation)),
                    "drive_count": updated_drive_count,
                    "updated_at": updated_at_timestamp,
                    "avg_contextual_speed_30_day": item.get("avg_contextual_speed_30_day", normalize_decimal(0)),
                    "avg_contextual_speed_60_day": item.get("avg_contextual_speed_60_day", normalize_decimal(0)),
                    "avg_contextual_speed_180_day": item.get("avg_contextual_speed_180_day", normalize_decimal(0)),
                }

                db_items_to_write.append({"PutRequest": {"Item": updated_item}})
            # ---------------------------------------------
            # CASE 2: NEW SEGMENT (NOT IN DATABASE)
            # ---------------------------------------------
            else:
                road['avg_traveling_speed'] = driver_avg_traveling_speed
                road['avg_speed_deviation'] = normalize_decimal(0)

                # Normalize OSM speed limit
                osm_speed_limit = road.get("osm_speed_limit")
                osm_speed_limit_is_unknown = (osm_speed_limit == "Unknown")

                # Convert OSM speed limit to normalize_decimal if numeric
                if isinstance(osm_speed_limit, (int, float)):
                    osm_speed_limit = normalize_decimal(str(osm_speed_limit))
                elif osm_speed_limit_is_unknown:
                    osm_speed_limit = normalize_decimal(0)

                road_segment_info = {
                    "road_segment_id": segment_id,
                    "osm_road_name": road['road_name'],
                    "osm_road_type": road['road_type'],
                    "avg_traveling_speed": driver_avg_traveling_speed, # Use current driver's avg traveling speed because it is the 1st entry in the table
                    "avg_speed_deviation": normalize_decimal(0),
                    "drive_count": int(1),
                    "avg_contextual_speed_30_day": normalize_decimal(0), 
                    "avg_contextual_speed_60_day": normalize_decimal(0), 
                    "avg_contextual_speed_180_day": normalize_decimal(0),
                    "updated_at": updated_at_timestamp  
                }
                
                # -------------------------------------------------------
                # If OSM speed limit is UNKNOWN → check Mapillary → then MapQuest
                # -------------------------------------------------------
                if osm_speed_limit_is_unknown: # Speed Limit from OSM is not present
                    segments_with_unknown_osm_speeds += 1
                    road["osm_speed_limit"] = 0  # normalize stored value
                    
                    # Check Mapillary
                    mapillary_result = map_speed_sign_to_nearest_road(road, mapillary_speed_signs)
                    mapillary_signs = mapillary_result.get("mapillary_speed_signs", [])

                    if mapillary_signs:
                        mapillary_speed_limit = normalize_decimal(mapillary_signs[0]["speed_limit"])
                        driver_speed_deviation = driver_avg_traveling_speed - mapillary_speed_limit

                        road_segment_info["avg_speed_deviation"] = driver_speed_deviation
                        road['avg_speed_deviation'] = driver_speed_deviation

                    else:
                        # Call MapQuest if still unknown - should only need to do this once per segment

                        mid_index = len(road['geometry']) // 2
                        middle_road_coord = (road['geometry'][mid_index]['lat'], road['geometry'][mid_index]['lon'])

                        mapquest_speed_limit = get_mapquest_speed_limit(middle_road_coord, config)
                        mapquest_api_counter += 1

                    
                        if mapquest_speed_limit != "Unknown":
                            mapquest_speed_limit = normalize_decimal(mapquest_speed_limit)
                            driver_speed_deviation = driver_avg_traveling_speed - mapquest_speed_limit

                            road_segment_info["avg_speed_deviation"] = driver_speed_deviation
                            road['avg_speed_deviation'] = driver_speed_deviation
                
                else:
                    driver_speed_deviation = driver_avg_traveling_speed - osm_speed_limit

                    road_segment_info['avg_speed_deviation'] = driver_speed_deviation
                    road['avg_speed_deviation'] = driver_speed_deviation

                db_items_to_write.append({"PutRequest": {"Item": road_segment_info}})
                # Write in batches
            
                if len(db_items_to_write) == batch_size:
                    batch_write_all(config.get('DYNAMODB_ROAD_SEGMENT_TABLE', 'drivenDB_road_segment_info'), db_items_to_write, config)
                    db_items_to_write = []  # Reset batch

    if db_items_to_write:
        batch_write_all(config.get('DYNAMODB_ROAD_SEGMENT_TABLE', 'drivenDB_road_segment_info'), db_items_to_write, config)

    # Determine True Speeding Events
    speeding_event_points = convert_points_for_speeding_events(geocode_to_segment, travelled_segments)
    grouped_events = driven_defined_speeding_events(speeding_event_points, config)

    resolve_speed_limits_end_time = time.time()
    elapsed_resolve_speed_limits = resolve_speed_limits_end_time - resolve_speed_limits_start_time

    final_output_functionality_start_time = time.time()

    speeding_records = []

    # Generate speeding records for contextualize speeds
    for (lat, lon, distracted, traveling_speed, timestamp), segment_data in geocode_to_segment.items():
        segment_id = segment_data['segment_id']
        segment = travelled_segments[segment_id]
        
        traveling_speed_dec = normalize_decimal(traveling_speed)
        avg_traveling_speed = normalize_decimal(segment['driver_avg_traveling_speed'])
        avg_speeding_deviation = normalize_decimal(segment.get('avg_speed_deviation', 0))

        driver_speed_deviation = traveling_speed_dec - avg_traveling_speed + avg_speeding_deviation

        fractional = driver_speed_deviation - int(driver_speed_deviation)

        if fractional < normalize_decimal(0.50):
            driver_speed_deviation = driver_speed_deviation.quantize(Decimal(1), rounding=ROUND_DOWN)
        else:
            driver_speed_deviation = driver_speed_deviation.quantize(Decimal(1), rounding=ROUND_UP)

        # Positive deviation indicates speeding
        if driver_speed_deviation > 0:
            speeding_records.append({
                "PutRequest": {
                    "Item": {
                        "road_segment_id": str(segment['id']),
                        "timestamp#user_id": f"{timestamp}#{user_id}",
                        "traveling_speed": traveling_speed_dec,
                        "speed_deviation": driver_speed_deviation 
                    }
                }
            })

    if speeding_records:
        batch_write_all(config.get('DYNAMODB_SPEEDING_EVENTS_TABLE', 'users_speeding_events'), speeding_records, config)


    final_output_functionality_end_time = time.time()
    elapsed_final_output_functionality_time = final_output_functionality_end_time - final_output_functionality_start_time
   
    algo_end_time = time.time()
    elapsed_algo_time = algo_end_time - algo_start_time

    return {
        'speeding_records': speeding_records,
        'grouped_events': grouped_events,
        'geocode_to_segment': geocode_to_segment,
        'travelled_segments': travelled_segments, 
        'road_history_stats': history_stats,
        'road_types_travelled': road_types_travelled,
        'metrics': {
            'user_geocodes': len(points),
            'travelled_segments': len(travelled_segments),
            'speeding_records': len(speeding_records),
            'unknown_speeds': segments_with_unknown_osm_speeds,
            'osm_api_calls': osm_api_call,
            'mapillary_speed_signs': len(mapillary_speed_signs),
            'mapquest_api_calls': mapquest_api_counter,
            'timings': {
                'reading_file': elapsed_reading_file_time,
                'mapillary_api': elapsed_mapillary_api_call_time,
                'determine_segments': elapsed_determine_travelled_segments,
                'resolve_speed_limits': elapsed_resolve_speed_limits,
                'road_history_tracking': elapsed_road_history_time,
                'final_output': elapsed_final_output_functionality_time,
                'total': elapsed_algo_time
            }
        }
    }

def detect_speeding_records(input_file, config=None):
    """
    Main function to detect speeding records from a given input file.
    
    Args:
        input_file (str): Path to the input file containing driving data
        config (dict): Configuration dictionary containing API keys and settings
        
    Returns:
        dict: Dictionary containing processed data and metrics
    """
    if config is None:
        config = {}
    return process_data_file(input_file, config)
