# Trip Summary Generation

This document describes the new trip summary generation functionality added to `detect_all_driving_events.py`.

## Overview

The trip summary generation creates a comprehensive JSON file containing all detected driving events, trip metadata, and waypoints. The output format matches the structure of the provided `trip_summary.json` example.

## Features

### Event Types (Enum System)
The following event types are supported:

0: "general_cornering"  
1: "speeding"  
2: "hard_braking"  
3: "rapid_acceleration"  
4: "hard_cornering"  
5: "distracted_driving"  
6: "night_driving"  
7: "road_familiarity"  
8: "road_type"  

### Trip Summary Structure

The generated JSON includes:

1. **Trip Metadata**:
   - Drive ID (from environment variable)
   - Device ID (from environment variable)
   - Account ID (from environment variable)
   - UTC offset (from environment variable)
   - Classification (from environment variable)
   - Start/end coordinates and timestamps
   - Total distance and duration

2. **Events Array**: All detected events with:
   - Event type (enum)
   - Timestamp
   - Location (lat/lon)
   - Speed (km/h)
   - Duration (seconds)
   - Risk level
   - Detailed event-specific data

3. **Waypoints Array**: All GPS points from the input file with:
   - Latitude/longitude
   - Timestamp
   - Speed data (km/h)
   - Speed limits (when available)

4. **Comprehensive Summary**: Statistics including:
   - Total events by type
   - Distance and duration
   - Road segment history (if `road_familiarity` is enabled)
   - Road types traveled (if `road_type` is enabled)
   - Late night driving time

## Usage

### Basic Usage
```bash
python detect_all_driving_events.py input_file.txt
```

### With Custom Output File
```bash
python detect_all_driving_events.py input_file.txt --output-summary my_trip_summary.json
```

### With Specific Events
```bash
python detect_all_driving_events.py input_file.txt --events speeding cornering --output-summary trip.json
```

## Output

The script will:
1. Process all enabled driving events
2. Generate a comprehensive trip summary
3. Save it as a JSON file (auto-named with timestamp or custom name)
4. Display summary statistics

Example output:
```
==================================================
GENERATING TRIP SUMMARY
==================================================
Trip summary saved to: trip_summary_20241201_143022.json
Trip Summary Stats:
  - Drive ID: BAF1C5FE-732A-427D-9A88-123149
  - Total Distance: 11.92 km
  - Total Events: 4
  - Speeding Events: 2
  - Cornering Events: 1
  - Accel/Decel Events: 1
  - Distracted Events: 0
  - Late Night Driving: 0 seconds
```

## Event Details

Each event in the summary includes relevant details:

### Speeding Events
- Average speed vs limit
- Maximum speed reached
- Speed excess amount
- Road type
- Number of data points

### Cornering Events
- Angular velocity (deg/s)
- Lateral acceleration (g)
- Event type (hard/general)
- Start/end locations

### Acceleration/Deceleration Events
- Event type (braking/acceleration)
- Maximum acceleration magnitude
- Start/end speeds

### Distracted Driving Events
- Consecutive distracted points
- Start/end indices
- Duration

### Late Night Driving
- Total late night seconds
- Percentage of trip
- Timezone used

### Road Familiarity (if enabled)
- Total segments in the trip
- Segments driven recently (within 21 days)
- Segments not driven recently
- Percentage of segments driven recently

### Road Types (if enabled)
- Count of road types traveled
- Breakdown by road type

## File Structure

The generated JSON follows this structure:
```json
{
  "driveid": "UUID",
  "distance_miles": 11.92,
  "duration_seconds": 1020,
  "driving": true,
  "id": 1234567890,
  "deviceid": "UUID",
  "utc_offset": "-04:00:00",
  "classification": "car",
  "start": {
    "ts": "2024-09-01T15:29:12Z",
    "lat": 27.9595481,
    "lon": -82.5333568
  },
  "end": {
    "ts": "2024-09-01T15:47:01Z", 
    "lat": 27.949466,
    "lon": -82.444008
  },
  "night_driving_seconds": 0,
  "account_id": "19857054769",
  "events": [...],
  "waypoints": [...],
  "summary": {...}
}
```

## Integration

The trip summary generation is automatically called at the end of the main processing pipeline. It uses all the detected events and metadata from the parallel processing to create a comprehensive summary.

## Dependencies

- `geopy` for distance calculations
- `uuid` for generating unique IDs
- `json` for output formatting
- All existing detection modules
