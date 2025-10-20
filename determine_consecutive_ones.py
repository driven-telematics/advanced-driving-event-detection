def analyze_consecutive_ones(file_path):
    # Read the input file
    with open(file_path, 'r') as file:
        data = file.read().strip()
    
    # Split data points by '|'
    data_points = data.split('|')
    
    # Extract the "distracted" values (3rd index in each data point)
    distracted_values = [int(point.split(',')[2]) for point in data_points]
    
    # Analyze consecutive ones
    events = []
    current_event_count = 0
    
    for value in distracted_values:
        if value == 1:
            current_event_count += 1
        else:
            if current_event_count > 3:
                events.append(current_event_count)
                current_event_count = 0
    
    # Add the last event if it exists
    if current_event_count > 3:
        events.append(current_event_count)
    
    # Total number of events and consecutive ones in each event
    total_events = len(events)
    consecutive_ones_per_event = events
    
    return total_events, consecutive_ones_per_event


# Example usage
file_path = 'test1.txt'  # Replace with your input file path
total_events, consecutive_ones_per_event = analyze_consecutive_ones(file_path)
print(f"Total number of events: {total_events}")
print(f"Consecutive ones per event: {consecutive_ones_per_event}")