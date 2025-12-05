# Driving Score Calculation Script

This Python script calculates a driving score based on telematics data, considering various driving behaviors such as speeding, distracted driving, hard braking, and night driving. The script is designed to help developers analyze driving patterns and generate a weighted driving score for a given session.

---

## Table of Contents
- [Overview](#overview)
- [Dependencies](#dependencies)
- [How It Works](#how-it-works)
- [Inputs](#inputs)
- [Outputs](#outputs)
- [Evaluation of the Scoring System](#evaluation-of-the-scoring-system)
- [Key Features](#key-features)
- [Future Enhancements](#future-enhancements)
---

## Overview

The script processes telematics data to:
1. Detect overlapping driving events (e.g., speeding while distracted).
2. Apply penalties and behavior factors to calculate scores for each driving behavior.
3. Generate a weighted final driving score for the session.
4. Optionally calculate a 21-day rolling average driving score using historical data.

## Dependencies
- Python Standard Library:
    - collections.Counter
    - datetime
    - json

---

## How It Works

1. **Event Normalization**: The script normalizes raw telematics data into a consistent format for processing.
2. **Overlap Detection**: It identifies overlapping events (e.g., speeding during night hours) based on business rules.
3. **Penalty Calculation**: Penalties are applied based on event durations and severity.
4. **Behavior Scores**: Scores are calculated for each behavior type (e.g., speeding, distracted driving).
5. **Weighted Final Score**: Behavior scores are weighted and summed to produce a final driving score.
6. **Rolling Average**: The script can calculate a 21-day rolling average driving score using historical data.

---

## Inputs

The script requires the following inputs:

1. **Telematics Data (`results`)**: A dictionary containing detected driving events. Example categories:
   - `accel_decel`: Hard braking and rapid acceleration events.
   - `distracted`: Distracted driving events.
   - `cornering`: Hard cornering events.
   - `speeding`: Speeding events with grouped data points.
   - `night_driving`: Night driving statistics.

2. **Configuration (`config`)**: A dictionary containing:
   - Penalty values for each behavior.
   - Behavior factors for overlapping events.
   - Weights for scoring.
   - Night driving time bounds and other thresholds.

3. **Total Seconds (`total_seconds`)**: Total duration of the driving session in seconds.

4. **Historical Data (TESTING PURPOSES)**: A JSON file containing past driving sessions for rolling average calculation.

---

## Outputs

The script generates the following outputs:

1. **Final Driving Score**: A weighted score (0–100) representing the overall driving performance.
2. **Behavior Scores**: Individual scores for each driving behavior (e.g., speeding, distracted driving).
3. **Detailed Breakdown**: A dictionary containing:
   - Total durations for each event type.
   - Total penalties applied.
   - Count of detected events.
4. **Star Ratings**: A visual representation of scores using stars (e.g., ★★★☆☆).

Example Output:
```json
{
  "final_driving_score": 85.75,
  "behavior_scores": {
    "distracted": 78.5,
    "speeding": 90.0,
    "hard_braking": 65.0,
    "rapid_acceleration": 88.0,
    "cornering": 92.0
  },
  "total_durations": {
    "distracted": 120,
    "speeding": 300
  },
  "total_penalties": {
    "distracted": 30.0,
    "speeding": 50.0
  },
  "total_events_detected_count": 15
}
```
## Evaluation of the Scoring System
The scoring system evaluates driving behavior and assigns a star rating based on the final driving score. The star rating system is as follows:

| **Score Range** | **Star Rating** |
|------------------|-----------------|
| 100             | ★★★★★          |
| 80–99           | ★★★★☆          |
| 60–79           | ★★★☆☆          |
| 40–59           | ★★☆☆☆          |
| Below 40        | ★☆☆☆☆          |

This system provides an intuitive way to evaluate driving performance, with higher scores indicating safer and more responsible driving.

## Key Features
- Event Overlap Detection: Identifies overlapping events based on business rules.
- Customizable Penalties and Factors: Easily configurable for different use cases.
- Weighted Scoring: Generates a final score based on weighted behavior scores.
- 21-Day Rolling Average: Calculates a weighted average score over the past 21 days.

## Future Implementation and To-Dos
1. **Database Integration**: Store and retrieve historical driving data from a database.
    - Insert new driving session record into database
    - Query Database for past 21 days of driving sessions
    - Recalculate 21_day_average driving score 
        - **Total seconds across drives in 21 day window**: Count of total duration across all drives in the new 21-day window
        - **Total driving scores**: Sum of all individual session scores
        - **Average driving score**: Sum of all individual session scores ÷ total 21 day duration
    - Update 21_day_average to user record

