# Detect Speeding Events

## Table of Contents
1. [Overview](#overview)
2. [Dependencies and Libraries](#dependencies-and-libraries)
3. [How It Works](#how-it-works)
4. [Inputs](#inputs)
5. [Outputs](#outputs)
6. [Key Features](#key-features)
7. [Future Enhancements](#future-enhancements)

---

## Overview
The `detect_speeding_events.py` script is designed to process driving data, detect speeding events, and analyze road segment history. It integrates with external APIs and databases to fetch road segment data, speed limits, and user driving history. The script identifies speeding events based on deviations from average speeds and predefined thresholds, while also maintaining a record of road segments driven in the past 21 days.

---

## Dependencies and Libraries
The script relies on the following libraries:
- **Python Standard Libraries**: `datetime`, `time`, `collections`
- **Third-Party Libraries**:
  - `boto3`: For interacting with AWS DynamoDB.
  - `requests`: For making HTTP requests to external APIs.
  - `geopy`: For calculating distances between geographical points.
  - `shapely`: For geometric calculations and operations.
  - `decimal`: For precise decimal arithmetic.

Ensure these libraries are installed before running the script. You can install them using `pip`:
```bash
pip install boto3 requests geopy shapely
```

---
## How It Works
1. **Data Processing**:
    - Reads driving data from an input file.
    - Extracts geographical coordinates, speed, and timestamps.
2. **Road Segment Detection**:
    - Queries OpenStreetMap (OSM) API to fetch road segments within a bounding box.
    - Matches user coordinates to the nearest road segment.
3. **Speed Limit Resolution**:
    - Fetches speed limits from OSM, Mapillary, and MapQuest APIs.
    - Calculates average traveling speed and speed deviations for each road segment.
4. **Speeding Event Detection**:
    - Identifies speeding events based on deviations from average speeds and thresholds.
    - Groups speeding events by duration and road type.
5. **Road Segment History**:
    - Tracks road segments driven in the past 21 days using DynamoDB.
    - Updates or creates new records for road segments.

---
## Inputs
1. **Driving Data File**:
    - A text file containing driving data points in the format:
    ```text
    lat,lon,distracted,traveling_speed,timestamp...|...
    ```

## Outputs
1. Speeding Records:
    - A list of speeding events with details such as start time, end time, duration, and speed deviations.
2. Metrics:
    - Statistics about the number of speeding records, unknown speed limits, API calls, and processing times.
        -  Road Types Traveled on.
3. Road Segment History:
    - Updated or new records in DynamoDB for road segments driven.
4. Grouped Events:
    - Grouped speeding events based on road type and duration.

## Key Features
- Road Segment Matching:
    - Matches user coordinates to the nearest road segment using OSM data.
- Speed Limit Resolution:
    - Resolves speed limits from multiple sources (OSM, Mapillary, MapQuest).
- Speeding Event Detection:
    - Detects speeding events based on configurable thresholds and durations.
- Road Segment History Tracking:
    - Tracks road segments driven in the past 21 days using DynamoDB with TTL.
- API Integration:
    - Integrates with OSM, Mapillary, and MapQuest APIs for road and speed limit data.
- Batch Processing:
    - Processes data in batches for scalability and efficiency.

## Future Enhancements
- **Scalable Implementation**
    - We will need to determine how we will interact with OSM and Mapillary as we scale. Right now, we batch API calls to improve performance, but this will exhaust the external systems because of the number of API calls we send.