# Advanced Driving Event Detection System

A comprehensive Python-based system for detecting and analyzing various driving events from GPS telematics data. This system processes driving sessions to identify speeding, cornering, acceleration/deceleration events, distracted driving, and night driving patterns, then calculates a weighted driving score based on detected behaviors.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Event Detection Modules](#event-detection-modules)
- [Driving Score Calculation](#driving-score-calculation)
- [Dependencies](#dependencies)
- [Environment Variables](#environment-variables)
- [Input Format](#input-format)
- [Output](#output)
- [Documentation](#documentation)
- [Testing](#testing)

## Overview

This system analyzes GPS telematics data to detect various driving events and patterns:

- **Speeding Events**: Identifies when drivers exceed speed limits using OSM, Mapillary, and MapQuest data
- **Cornering Events**: Detects general and hard cornering based on angular velocity and lateral acceleration
- **Acceleration/Deceleration**: Identifies hard braking and rapid acceleration events
- **Distracted Driving**: Detects periods of distracted driving based on consecutive data points
- **Night Driving**: Tracks driving during late-night hours (configurable time window)
- **Road Familiarity**: Tracks which road segments were driven recently (21-day window)
- **Road Type Classification**: Categorizes roads traveled during the trip

The system then calculates a comprehensive driving score based on detected events, overlaps, and configurable penalty values.

## Features

### Event Detection

- ✅ **Parallel Processing**: Multiple event types are detected concurrently for improved performance
- ✅ **Configurable Thresholds**: All detection thresholds can be customized via environment variables
- ✅ **Road Segment Matching**: Intelligent matching of GPS coordinates to road segments
- ✅ **Multi-Source Speed Limits**: Combines data from OSM, Mapillary, and MapQuest APIs
- ✅ **Road History Tracking**: Tracks road segments driven in the past 21 days using DynamoDB
- ✅ **Event Overlap Detection**: Identifies when multiple events occur simultaneously

### Scoring System

- ✅ **Weighted Scoring**: Configurable weights for different driving behaviors
- ✅ **Behavior Factors**: Multipliers applied when events overlap (e.g., speeding while distracted)
- ✅ **Penalty-Based System**: Configurable penalty values for each event type
- ✅ **21-Day Rolling Average**: Calculates weighted average score over rolling window
- ✅ **Star Rating System**: Visual representation of driving performance (1-5 stars)

### Trip Summary

- ✅ **Comprehensive JSON Output**: Complete trip summary with all events and metadata
- ✅ **Waypoint Extraction**: All GPS points with enriched data (speed limits, road types, etc.)
- ✅ **Distance & Duration Calculation**: Accurate trip distance and duration metrics

## Installation

### Prerequisites

- Python 3.8 or higher
- AWS credentials configured (for DynamoDB access)
- API keys for external services (see [Environment Variables](#environment-variables))

### Setup

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd advanced-driving-event-detection
   ```

2. **Create and activate a virtual environment**:
   ```bash
   python -m venv myenv
   source myenv/bin/activate  # On Windows: myenv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install boto3 requests geopy shapely python-dotenv aiohttp mercantile
   ```

   Or install all at once:
   ```bash
   pip install boto3 requests geopy shapely python-dotenv aiohttp mercantile
   ```

4. **Create a `.env` file** in the project root (see [Environment Variables](#environment-variables) section)

5. **Configure AWS credentials** for DynamoDB access:
   ```bash
   aws configure --profile local-dynamo-db-access
   ```

## Configuration

The system is configured via environment variables in a `.env` file. Create a `.env` file in the project root with the following variables:

### Required API Keys

```env
MAPILLARY_ACCESS_TOKEN=your_mapillary_token
MAPQUEST_API_KEY=your_mapquest_key
```

### Database Configuration

```env
DYNAMODB_ROAD_SEGMENT_TABLE=drivenDB_road_segment_info
DYNAMODB_SPEEDING_EVENTS_TABLE=users_speeding_events
DRIVEN_OVERPASS_URL=https://maps.driven-api.com/api/interpreter
```

### Event Detection Thresholds

```env
# Speeding
EXCESS_SPEED_THRESHOLD_MPH=11
EXCESS_SPEED_DURATION_SECONDS=5

# Cornering
GENERAL_TURN_THRESHOLD_DEG_S=15
HARD_TURN_THRESHOLD_DEG_S=75
GENERAL_LATERAL_ACCEL_G=0.1
HARD_LATERAL_ACCEL_G=0.4
TURNING_TIME_LIMIT=15
COOLDOWN_PERIOD=3

# Acceleration/Deceleration
BRAKING_THRESHOLD=9.0
ACCEL_THRESHOLD=7.0

# Distracted Driving
DISTRACTED_MIN_DURATION_SECONDS=5
DISTRACTED_MIN_SPEED_MPH=10.0

# Night Driving
LOWER_BOUND_DRIVE_HOUR=0
UPPER_BOUND_DRIVE_HOUR=4
UTC_OFFSET=-04:00:00

# Batch Sizes
BATCH_SIZE=20
DB_BATCH_SIZE=25

# User Information
USER_ID=31399
ACCOUNT_ID=19857054769
DEVICE_ID=your_device_id
DRIVE_ID=your_drive_id
CLASSIFICATION=car
```

### Event Enablement Toggles

```env
ENABLE_SPEEDING=true
ENABLE_ROAD_FAMILIARITY=true
ENABLE_ROAD_TYPES=true
ENABLE_CORNERING=true
ENABLE_HARD_BRAKING=true
ENABLE_RAPID_ACCELERATION=true
ENABLE_DISTRACTED=true
ENABLE_NIGHT_DRIVING=true
```

### Scoring Configuration

See the [Driving Score Calculation](#driving-score-calculation) section for penalty values, behavior factors, and weights configuration.

## Usage

### Basic Usage

Process all driving events from an input file:

```bash
python detect_all_driving_events.py tests/test1.txt
```

### Select Specific Events

Process only specific event types:

```bash
python detect_all_driving_events.py tests/test1.txt --events speeding cornering
```

Available event types:
- `speeding`
- `cornering`
- `accel_decel`
- `distracted`
- `night_driving`
- `all` (default)

### Custom Output File

Specify a custom output file for the trip summary:

```bash
python detect_all_driving_events.py tests/test1.txt --output-summary my_trip.json
```

### Parallel Processing

Adjust the number of parallel workers:

```bash
python detect_all_driving_events.py tests/test1.txt --max-workers 5
```

### Individual Module Usage

Each detection module can also be run independently:

```bash
# Speeding events only
python detect_speeding_events.py tests/test1.txt

# Cornering events only
python detect_cornering_events.py tests/test1.txt

# Acceleration/deceleration events
python detect_accel_decel_events.py tests/test1.txt

# Distracted driving events
python detect_distracted_events.py tests/test1.txt

# Night driving events
python detect_night_driving_events.py tests/test1.txt
```

## Project Structure

```
advanced-driving-event-detection/
├── detect_all_driving_events.py    # Main orchestrator script
├── calculate_driving_score.py      # Driving score calculation
├── helper_functions.py              # Utility functions
├── mapillary_query_optimization.py # Mapillary API optimization
│
├── Event Detection Modules:
│   ├── detect_speeding_events.py
│   ├── detect_cornering_events.py
│   ├── detect_accel_decel_events.py
│   ├── detect_distracted_events.py
│   └── detect_night_driving_events.py
│
├── Documentation/
│   ├── CALCULATE_DRIVING_SCORE_README.md
│   ├── DETECT_SPEEDING_EVENTS_README.md
│   ├── MAPILLARY_QUERY_OPTIMIZATION_README.md
│   └── TRIP_SUMMARY_README.md
│
├── tests/
│   ├── test*.txt                   # Sample input files
│   └── *.json                      # Sample output files
│
├── .env                            # Environment variables (create this)
└── README.md                       # This file
```

## Event Detection Modules

### 1. Speeding Events (`detect_speeding_events.py`)

Detects when drivers exceed speed limits by:
- Matching GPS coordinates to road segments using OSM data
- Resolving speed limits from multiple sources (OSM, Mapillary, MapQuest)
- Calculating speed deviations from posted limits
- Grouping consecutive speeding instances into events
- Tracking road segment history (21-day window)

**Key Features**:
- Road segment matching with distance calculations
- Multi-source speed limit resolution
- Configurable excess speed thresholds
- Road type classification
- Road familiarity tracking

### 2. Cornering Events (`detect_cornering_events.py`)

Identifies cornering maneuvers based on:
- Angular velocity (degrees per second)
- Lateral acceleration (g-forces)
- Configurable thresholds for general vs. hard cornering

**Event Types**:
- General Cornering: Moderate turns
- Hard Cornering: Aggressive turns exceeding higher thresholds

### 3. Acceleration/Deceleration Events (`detect_accel_decel_events.py`)

Detects rapid changes in velocity:

**Event Types**:
- **Hard Braking**: Deceleration exceeding threshold (default: 9.0 mph/s)
- **Rapid Acceleration**: Acceleration exceeding threshold (default: 7.0 mph/s)

### 4. Distracted Driving Events (`detect_distracted_events.py`)

Identifies periods of distracted driving:
- Requires minimum consecutive distracted data points
- Minimum speed threshold to filter out stationary periods
- Configurable duration thresholds

### 5. Night Driving Events (`detect_night_driving_events.py`)

Tracks driving during late-night hours:
- Configurable time window (default: 12 AM - 4 AM)
- Timezone-aware calculations
- Percentage of trip during night hours
- Total night driving duration

## Driving Score Calculation

The system calculates a comprehensive driving score based on detected events. The scoring system works as follows:

### 1. Event Detection & Overlap Analysis

- Detects all driving events from telematics data
- Identifies overlapping events (e.g., speeding while distracted)
- Applies business rules for valid overlaps

### 2. Penalty Calculation

Each event type has a base penalty value (per second or per occurrence):

| Event Type | Default Penalty |
|------------|----------------|
| Distracted Driving | 26.0 |
| Speeding | 26.0 |
| Hard Braking | 600.0 |
| Rapid Acceleration | 600.0 |
| Hard Cornering | 1200.0 |
| Night Driving | 2.0 per second |
| Road Familiarity | 1.05 per new segment |
| Road Type | 1.05 per classified segment |

### 3. Behavior Factors

When events overlap, behavior factors multiply the penalty:

**Distracted Driving**:
- Speeding > 55 mph: 2.5x
- Hard Braking: 2.0x
- Night Driving: 3.0x

**Speeding**:
- Distracted: 2.5x
- Hard Braking: 2.0x
- Night Driving: 2.5x
- Severe Speeding (>20 mph over): 2.5x

**Hard Braking**:
- Distracted: 2.0x
- Speeding: 2.0x
- Night Driving: 2.5x

**Rapid Acceleration**:
- Speeding: 1.5x
- Night Driving: 3.0x

**Cornering**:
- Night Driving: 3.0x

### 4. Behavior Scores

Individual scores calculated for each behavior:

```
Behavior Score = (Total Time - Total Penalty) / Total Time × 100
```

For road familiarity and road type:
```
Score = (Total Segments - Total Penalty) / Total Segments × 100
```

### 5. Weighted Final Score

Final score is a weighted sum of behavior scores:

| Behavior | Default Weight |
|----------|---------------|
| Distracted Driving | 38% |
| Speeding | 25% |
| Hard Braking | 18% |
| Rapid Acceleration | 8% |
| Hard Cornering | 4% |
| Night Driving | 4% |
| Road Familiarity | 1.5% |
| Road Type | 1.5% |

### 6. Star Rating

Final score is converted to a star rating:

- 100: ★★★★★
- 80-99: ★★★★☆
- 60-79: ★★★☆☆
- 40-59: ★★☆☆☆
- <40: ★☆☆☆☆

### Configuration

All penalty values, behavior factors, and weights can be configured via environment variables. See the [Configuration](#configuration) section for details.

## Dependencies

### Python Standard Library

- `argparse`
- `concurrent.futures`
- `datetime`
- `json`
- `os`
- `time`
- `collections`
- `decimal`
- `re`

### Third-Party Libraries

- **boto3**: AWS SDK for DynamoDB access
- **requests**: HTTP library for API calls
- **geopy**: Geographic distance calculations
- **shapely**: Geometric operations for road segment matching
- **python-dotenv**: Environment variable management
- **aiohttp**: Asynchronous HTTP client (for Mapillary optimization)
- **mercantile**: Tile-based geographic calculations

Install all dependencies:

```bash
pip install boto3 requests geopy shapely python-dotenv aiohttp mercantile
```

## Environment Variables

See the [Configuration](#configuration) section for a complete list of environment variables. Key variables include:

- **API Keys**: `MAPILLARY_ACCESS_TOKEN`, `MAPQUEST_API_KEY`
- **Database**: `DYNAMODB_ROAD_SEGMENT_TABLE`, `DYNAMODB_SPEEDING_EVENTS_TABLE`
- **Detection Thresholds**: Various thresholds for each event type
- **Scoring Configuration**: Penalties, behavior factors, weights
- **Trip Metadata**: `DRIVE_ID`, `DEVICE_ID`, `ACCOUNT_ID`, etc.

## Input Format

The system expects input files in a pipe-delimited format, where each record contains:

```
lat,lon,distracted,speed,timestamp
```

Multiple records are separated by pipe (`|`) characters.

**Example**:
```
36.096799,-115.172748,0,32.0,1760645907|36.096800,-115.172749,0,33.0,1760645908|36.096801,-115.172750,1,34.0,1760645909
```

**Field Descriptions**:
- `lat`: Latitude (decimal degrees)
- `lon`: Longitude (decimal degrees)
- `distracted`: Binary flag (0 = not distracted, 1 = distracted)
- `speed`: Traveling speed (mph)
- `timestamp`: Unix timestamp (seconds)

## Output

### Console Output

The system prints detailed information about detected events:

- Event counts by type
- Speeding event details (location, speed, duration)
- Cornering events with angular velocity and acceleration
- Acceleration/deceleration events
- Distracted driving periods
- Night driving statistics
- Road segment history
- Road types traveled
- Comprehensive summary with totals
- Driving score with star rating

### Trip Summary JSON

A comprehensive JSON file is generated containing:

- **Trip Metadata**: Drive ID, device ID, distance, duration, start/end points
- **Events Array**: All detected events with timestamps, locations, and details
- **Waypoints Array**: All GPS points with enriched data
- **Summary**: Event counts and statistics

Example output file: `trip_summary_20241201_143022.json`

### Driving Score Output

The scoring system outputs:

- Final driving score (0-100)
- Individual behavior scores
- Star ratings for each behavior
- Overlap detection results
- Penalty breakdowns
- 21-day rolling average (if historical data available)

## Documentation

Additional detailed documentation is available in the `Documentation/` directory:

- **[TRIP_SUMMARY_README.md](Documentation/TRIP_SUMMARY_README.md)**: Trip summary generation details
- **[CALCULATE_DRIVING_SCORE_README.md](Documentation/CALCULATE_DRIVING_SCORE_README.md)**: Scoring system documentation
- **[DETECT_SPEEDING_EVENTS_README.md](Documentation/DETECT_SPEEDING_EVENTS_README.md)**: Speeding detection details
- **[MAPILLARY_QUERY_OPTIMIZATION_README.md](Documentation/MAPILLARY_QUERY_OPTIMIZATION_README.md)**: API optimization strategies

## Testing

Sample test files are available in the `tests/` directory. To test the system:

```bash
# Process a sample test file
python detect_all_driving_events.py tests/test1.txt

# Process with specific events
python detect_all_driving_events.py tests/test1.txt --events speeding cornering

# Generate trip summary with custom name
python detect_all_driving_events.py tests/test1.txt --output-summary test_output.json
```

## Performance

- **Parallel Processing**: Multiple event types are detected concurrently
- **Batch API Calls**: Road segment data is fetched in batches
- **Caching**: Mapillary API responses are cached to reduce calls
- **Configurable Workers**: Adjust parallel worker count based on system resources

## Future Enhancements

- Database integration for storing historical driving sessions
- Improved API rate limiting and caching strategies
- Real-time processing capabilities
- Additional event detection types
- Enhanced road segment matching algorithms
- Machine learning-based anomaly detection

