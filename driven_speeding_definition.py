import datetime
from geopy.distance import geodesic

def parse_data(file_path):
    points = []
    with open(file_path, 'r') as file:
        file_content = file.read()
    
    for entry in file_content.strip().split('|'):
        if entry:
            lat, long, distracted, speed, limit, road_type, timestamp = entry.split(',')
            points.append({
                'lat': float(lat),
                'long': float(long),
                'distracted': bool(int(distracted)),
                'speed': float(speed),
                'limit': float(limit),
                'road_type': road_type,
                'timestamp': int(timestamp)
            })
    return points

def driven_defined_speeding_events(points):
    """Detects speeding events where the driver exceeds the speed limit by 11+ mph for at least 5 seconds."""
    speeding_events = []
    current_event = []
    speeding_event_counter = 0
    start_time = None 
    
    for point in points:
        excess_speed = point['speed'] - point['limit']
        
        if excess_speed >= 11 and point['limit'] > 0:
            if not current_event: # start of a new speeding event 
                start_time = point['timestamp'] 
            current_event.append(point) 
        else:
            # If we were in a speeding event and now we're not, check if it met the 5s requirement
            if current_event and start_time is not None and (current_event[-1]['timestamp'] - start_time) >= 5:
                speeding_events.append(current_event.copy()) 
                speeding_event_counter += 1
            current_event = [] 
            start_time = None  
    
    # Final check in case the last event meets the requirement
    if current_event and start_time is not None and (current_event[-1]['timestamp'] - start_time) >= 5:
        speeding_events.append(current_event)

    return speeding_events, speeding_event_counter


def calculate_road_statistics(points, speeding_events):
    """Calculate statistics of speeding events by road type and return percentage of speeding events over total trip distance or time."""
    road_segments = {}
    total_distance_travelled = 0
    speeding_distance = 0
    total_time = 0
    speeding_time = 0
    
    # Group points by road type and calculate distances and times
    previous_point = None
    for point in points:
        road_type = point['road_type']
        if previous_point:
            distance = geodesic((previous_point['lat'], previous_point['long']), (point['lat'], point['long'])).miles
            # Update distance and time for each road type
            # setdefault() method returns the value of a key if in dictionary, if not, it inserts the key with specified value
            road_segments.setdefault(road_type, {'distance': 0, 'speeding_distance': 0, 'time': 0, 'speeding_time': 0})['distance'] += distance
            road_segments[road_type]['time'] += (point['timestamp'] - previous_point['timestamp'])
            
            # Update total distance and time of entire trip
            total_distance_travelled += distance
            total_time += (point['timestamp'] - previous_point['timestamp'])
        
        previous_point = point
    
    # Calculate speeding events for each road type
    for event in speeding_events:
        event_start = event[0]
        event_end = event[-1]
        event_road_type = event_start['road_type']
        
        event_distance = 0
        event_time = event_end['timestamp'] - event_start['timestamp']
        
        # Calculate total distance of an event by summing up distances between consecutive points
        for i in range(1, len(event)):
            event_distance += geodesic((event[i-1]['lat'], event[i-1]['long']), (event[i]['lat'], event[i]['long'])).miles
        
        road_segments[event_road_type]['speeding_distance'] += event_distance
        road_segments[event_road_type]['speeding_time'] += event_time
        speeding_distance += event_distance
        speeding_time += event_time
    
    # Calculate the percentage of speeding events by road type
    road_percentages = {}
    for road_type, stats in road_segments.items():
        if total_distance_travelled > 0:
            road_percentages[road_type] = {
                'speeding_distance_percentage': (stats['speeding_distance'] / total_distance_travelled) * 100,
                'speeding_time_percentage': (stats['speeding_time'] / total_time) * 100
            }
    
    minutes = total_time // 60
    seconds = total_time % 60
    
    return road_percentages, total_distance_travelled, speeding_distance, speeding_time, (minutes, seconds)


file_path = "./speed_data.txt" 
data_points = parse_data(file_path)
speeding_events, speeding_event_count = driven_defined_speeding_events(data_points)
road_percentages, trip_distance, speeding_distance, speeding_time, (trip_duration_minutes, trip_duration_seconds) = calculate_road_statistics(data_points, speeding_events)

for event in speeding_events:
    print("Speeding Event Detected:")
    print(f"Start Time: {datetime.datetime.fromtimestamp(event[0]['timestamp'])}")
    print(f"End Time: {datetime.datetime.fromtimestamp(event[-1]['timestamp'])}")
    print(f"Speeding Duration: {event[-1]['timestamp'] - event[0]['timestamp']} seconds")
    print("Details:")
    for point in event:
        print(f"  Time: {datetime.datetime.fromtimestamp(point['timestamp'])}, Speed: {point['speed']} mph, Limit: {point['limit']} mph, Road Type: {point['road_type']}, Location: ({point['lat']}, {point['long']})")
    print("-" * 50)

print(f"Total Trip Distance: {trip_distance:.2f} miles")
print(f"Total Trip Duration: {trip_duration_minutes} minutes {trip_duration_seconds} seconds")
print(f"Total # of Speeding Events Detected: {speeding_event_count}")
print(f"Total Distance Speeding(Across Entire Trip): {speeding_distance:.2f} miles")
print(f"Total Time Speeding(Across Entire Trip): {speeding_time} seconds")
print("-" * 50)
for road_type, percentages in road_percentages.items():
    print(f"Road Type: {road_type}")
    print(f"  Speeding Distance Percentage(of Entire Trip): {percentages['speeding_distance_percentage']:.2f}%")
    print(f"  Speeding Time Percentage(of Entire Trip): {percentages['speeding_time_percentage']:.2f}%\n")

