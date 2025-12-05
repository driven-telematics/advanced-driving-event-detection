from collections import Counter
from datetime import datetime, timedelta, timezone
from helper_functions import read_dict_from_json, convert_utc_offset_to_hours, local_to_utc_hour

"""
results: dict containing detected events

    accel/decel[]:
    {
        "type":"Hard Braking",
        "start":{
            "lat":36.096799,
            "lon":-115.172748,
            "velocity":32.0,
            "timestamp":1760645907,
            "accel_mphs":22.9080681755466
        },
        "end":{
            "lat":36.096799,
            "lon":-115.172748,
            "velocity":32.0,
            "timestamp":1760645907,
            "accel_mphs":22.9080681755466
        },
        "max_accel":22.9080681755466
    },
    {
        "type":"Rapid Acceleration",
        "start":{
            "lat":36.108003,
            "lon":-115.157652,
            "velocity":34.0,
            "timestamp":1760646245,
            "accel_mphs":30.9455203051482
        },
        "end":{
            "lat":36.108003,
            "lon":-115.157652,
            "velocity":34.0,
            "timestamp":1760646245,
            "accel_mphs":30.9455203051482
        },
        "max_accel":30.9455203051482
    }

    distracted[]:
        {
            "start_idx":241,
            "end_idx":246,
            "start_time":1746468979,
            "end_time":1746468987,
            "length":8
        }

    cornering[]:
        {
            "event_type":"GENERAL_CORNER",
            "start_location":(36.101739,-115.150073),
            "end_location":(36.101446,-115.150139),
            "start_time_unix":1760645196,
            "end_time_unix":1760645199,
            "duration":3,
            "angular_velocity_deg_s":2.56,
            "lateral_acceleration_g":1.023
        }

    # might need to add start_time and end_time for overlap detection
    night_driving{}: 
        {
            "total_night_driving_seconds":0,
            "total_night_driving_minutes":0.0,
            "total_night_driving_hours":0.0,
            "total_points":456,
            "night_driving_points":0,
            "night_driving_percentage":0.0
        }

    speeding{}:
        grouped_events[events[]]:
            {
            "lat":29.748756,
            "long":-95.774785,
            "distracted":false,
            "speed":80.0,
            "limit":60.0,
            "road_type":"motorway",
            "timestamp":1746468891
            },
            {
            "lat":29.748553,
            "long":-95.774664,
            "distracted":false,
            "speed":80.0,
            "limit":60.0,
            "road_type":"motorway",
            "timestamp":1746468892
            },
            {
            "lat":29.748351,
            "long":-95.774544,
            "distracted":false,
            "speed":80.0,
            "limit":60.0,
            "road_type":"motorway",
            "timestamp":1746468893
            },
            {
            "lat":29.747948,
            "long":-95.774303,
            "distracted":false,
            "speed":80.0,
            "limit":60.0,
            "road_type":"motorway",
            "timestamp":1746468895
            },
            {
            "lat":29.747543,
            "long":-95.7741,
            "distracted":false,
            "speed":80.0,
            "limit":50.0,
            "road_type":"motorway_link",
            "timestamp":1746468897
            }

    Business Logic:
    - Distracted event can overlap with speeding events, harsh braking events, and night driving events (travelling speed of event is > 55 mph). 
    - Speeding event can overlap with distracted, harsh braking, and night hours events. We also need to check if the speeding event is in excess of 20 mph of speed limit.
    - Harsh Braking event can overlap with distracted, speeding, and night driving events.
    - Rapid Acceleration event can overlap with speeding and night driving events.
    - Hard Cornerning can overlap with night driving events.

    Other Context:
    - Night driving is defined as driving between LOWER_BOUND_DRIVE_HOUR and UPPER_BOUND_DRIVE_HOUR in config. 
"""

def is_night_driving(ts_unix: int, lower_bound: int, upper_bound: int) -> bool:
    """
    Determine if a timestamp (unix) falls within night hours.
    """
    if ts_unix is None:
        return False

    hour = datetime.fromtimestamp(ts_unix, tz=timezone.utc).hour 
    if lower_bound > upper_bound: 
        return hour >= lower_bound or hour < upper_bound
    return lower_bound <= hour < upper_bound


def event_spans_night(start: int, end: int, lower_bound: int, upper_bound: int) -> bool:
    """
    Check if an event (start..end) occurs during night hours.
    Uses sampling of start, mid, and end timestamps.
    """
    if not start or not end:
        return False
    if start > end:
        start, end = end, start
    mid = int((start + end) // 2)
    return (
        is_night_driving(start, lower_bound, upper_bound)
        or is_night_driving(mid, lower_bound, upper_bound)
        or is_night_driving(end, lower_bound, upper_bound)
    )


def time_overlap(a_start: int, a_end: int, b_start: int, b_end: int) -> bool:
    """Return True if two time intervals overlap."""
    if not all([a_start, a_end, b_start, b_end]):
        return False
    return (a_start <= b_end) and (b_start <= a_end)


def detect_overlapping_events(results: dict, config: dict) -> list:
    """
    Detect overlaps between telematics events according to business rules.
    Returns a list of overlap records.
    """
    lower_bound = config.get("LOWER_BOUND_DRIVE_HOUR", 0)
    upper_bound = config.get("UPPER_BOUND_DRIVE_HOUR", 4)
    utc_offset_str = config.get("UTC_OFFSET", "-05:00:00")
    max_allowed_speed_threshold = config.get("MAX_ALLOWED_SPEED_THRESHOLD_MPH", 55)
    severe_speed_threshold = config.get("SEVERE_SPEED_THRESHOLD_MPH_OVER_LIMIT", 20)
    offset_hours = convert_utc_offset_to_hours(utc_offset_str)
    night_lower_utc = local_to_utc_hour(lower_bound, offset_hours)
    night_upper_utc = local_to_utc_hour(upper_bound, offset_hours)

    OVERLAP_RULES = {
        "distracted": ["speeding", "hard_braking", "night_driving"],
        "speeding": ["distracted", "hard_braking", "night_driving"],
        "hard_braking": ["distracted", "speeding", "night_driving"],
        "rapid_acceleration": ["speeding", "night_driving"],
        "cornering": ["night_driving"],
    }

    normalized = []

    # ---- Normalize each event category ----

    # accel/decel events
    for idx, e in enumerate(results.get("accel_decel", [])):
        start_time = e.get("start", {}).get("timestamp")
        end_time = e.get("end", {}).get("timestamp", start_time)
        etype = e.get("type", "accel_decel").lower().replace(" ", "_")
        normalized.append({
            "id": f"accel_{idx}",
            "category": etype,
            "start": start_time,
            "end": end_time,
            "duration": 1 if (end_time - start_time) == 0 else (end_time - start_time),
            "meta": e,
            "spans_night": event_spans_night(start_time, end_time, night_lower_utc, night_upper_utc)
        })

    # distracted events
    for idx, e in enumerate(results.get("distracted", [])):
        start_time, end_time = e.get("start_time"), e.get("end_time")
        normalized.append({
            "id": f"distracted_{idx}",
            "category": "distracted",
            "start": start_time,
            "end": end_time,
            "duration": 1 if (end_time - start_time) == 0 else (end_time - start_time),
            "meta": e,
            "spans_night": event_spans_night(start_time, end_time, night_lower_utc, night_upper_utc)
        })

    # cornering events
    for idx, e in enumerate(results.get("cornering", [])):
        start_time, end_time = e.get("start_time_unix"), e.get("end_time_unix")
        normalized.append({
            "id": f"corner_{idx}",
            "category": "cornering",
            "start": start_time,
            "end": end_time,
            "duration": 1 if (end_time - start_time) == 0 else (end_time - start_time),
            "meta": e,
            "spans_night": event_spans_night(start_time, end_time, night_lower_utc, night_upper_utc)
        })

    # speeding events
    for idx, event in enumerate(results.get("speeding", {}).get("grouped_events", [])):
        if not event or "points" not in event:
            continue

        points = event["points"]
        start_time = event.get("start_time", points[0].get("timestamp") if points else None)
        end_time = event.get("end_time", points[-1].get("timestamp") if points else None)

        is_over_max_speed_allowed = any(
            p.get("driver_speed_deviation", 0) > max_allowed_speed_threshold
            for p in points
        )

        is_severe_speeding = any(
            p.get("speed", 0) > severe_speed_threshold
            for p in points
        )

        spans_night = any(
            is_night_driving(p.get("timestamp"), night_lower_utc, night_upper_utc)
            for p in points
        ) or event_spans_night(start_time, end_time, night_lower_utc, night_upper_utc)

        normalized.append({
            "id": event.get("event_id", f"speeding_{idx}"),
            "category": "speeding",
            "start": start_time,
            "end": end_time,
            "duration": 1 if (end_time - start_time) == 0 else (end_time - start_time),
            "meta": {
                "events": points,
                "is_over_max_speed_allowed": is_over_max_speed_allowed,
                "is_severe_speeding": is_severe_speeding,
                "start_speed": event.get("start_speed"),
                "end_speed": event.get("end_speed"),
            },
            "spans_night": spans_night
        })

    # --------------------------------------------------

    all_events_detected = []
    has_overlap = set()

    for i, event_A in enumerate(normalized):

        # Night driving overlap detection (implicit)
        if event_A.get("spans_night"):
            has_overlap.add(event_A["id"])
            all_events_detected.append({
                "primary_event_id": event_A["id"],
                "primary_event_category": event_A["category"],
                "primary_event_duration": event_A["duration"],
                "overlapping_event_category": "night_driving"
            })
                

        for j, event_B in enumerate(normalized):
            if i == j:
                continue

            # Skip disallowed combinations
            if event_B["category"] not in OVERLAP_RULES.get(event_A["category"], []):
                continue

            # Time-based overlap
            if time_overlap(event_A["start"], event_A["end"], event_B["start"], event_B["end"]):
                
                if event_A["category"] == 'distracted' and event_B["category"] == 'speeding':
                    # Only count overlap if traveling speed > 55 mph
                    if event_B["meta"].get("is_over_max_speed_allowed", False):
                        has_overlap.add(event_A["id"])
                        has_overlap.add(event_B["id"])

                        all_events_detected.append({
                            "primary_event_id": event_A["id"],
                            "primary_event_category": event_A["category"],
                            "primary_event_duration": event_A["duration"],
                            "overlapping_event_category": "over_max_speed_allowed",
                            "overlapping_event_id": event_B["id"]
                        })
                
                elif event_A["category"] == 'speeding':

                    if event_A["meta"].get("is_severe_speeding", False):
                        has_overlap.add(event_A["id"])

                        all_events_detected.append({
                            "primary_event_id": event_A["id"],
                            "primary_event_category": event_A["category"],
                            "primary_event_duration": event_A["duration"],
                            "overlapping_event_category": "severe_speeding",
                            "overlapping_event_id": None
                        })
                else:
                    has_overlap.add(event_A["id"])
                    has_overlap.add(event_B["id"])
                    
                    all_events_detected.append({
                        "primary_event_id": event_A["id"],
                        "primary_event_category": event_A["category"],
                        "primary_event_duration": event_A["duration"],
                        "overlapping_event_category": event_B["category"],
                        "overlapping_event_id": event_B["id"]
                    })
    
    # Add events that never overlap with any other event
    for event in normalized:
        if not event["id"] in has_overlap:
            all_events_detected.append({
                "primary_event_id": event["id"],
                "primary_event_category": event["category"],
                "primary_event_duration": event["duration"],
                "overlapping_event_category": None,
                "overlapping_event_id": None
            })

    return all_events_detected


def calculate_weighted_overall_driving_score(
    current_seconds_driven: int,
    current_driving_score: float,
    json_file_path: str
):
    """
    Calculates the weighted average driving score over a 21-day rolling window.
    Weight = seconds driven.
    """
    sessions = read_dict_from_json(json_file_path)
    now = datetime.now(timezone.utc)
    rolling_window_start = now - timedelta(days=21)

    weighted_score_total = 0.0
    weight_sum = 0

    for s in sessions:
        start_time = datetime.strptime(s["start_time"], "%Y-%m-%dT%H:%M:%SZ")
        start_time = start_time.replace(tzinfo=timezone.utc)

        if start_time >= rolling_window_start:
            seconds = s.get("seconds_driven", 0)
            score = s.get("driving_score", 0.0)

            weighted_score_total += score * seconds
            weight_sum += seconds

    # Add current session
    weighted_score_total += current_driving_score * current_seconds_driven
    weight_sum += current_seconds_driven

    if weight_sum == 0:
        print("No data available in rolling window.")
        return None

    weighted_avg = weighted_score_total / weight_sum
    print(f"Weighted overall driving score (21-day): {weighted_avg:.5f}")

    return weighted_avg

def get_stars(score: float) -> str:
    if score == 100:
        return "★★★★★"
    elif 80 <= score <= 99:
        return "★★★★☆"
    elif 60 <= score <= 79:
        return "★★★☆☆"
    elif 40 <= score <= 59:
        return "★★☆☆☆"
    else:
        return "★☆☆☆☆"

def print_star_ratings(driving_data: dict):
    print("🚗 Final Driving Score:")
    print(f"  {driving_data['final_driving_score']:.2f} → {get_stars(driving_data['final_driving_score'])}\n")

    print("📊 Behavior Scores:")
    for behavior, score in driving_data.get("behavior_scores", {}).items():
        print(f"  {behavior.replace('_', ' ').title()}: {score:.2f} → {get_stars(score)}")


def calculate_driving_score(results, config, total_seconds):

    penalty_values = config['penalty_values']
    behavior_factors = config['behavior_factors']
    weights = config['weights']
    all_events_detected = detect_overlapping_events(results, config)
    total_road_segments = results.get('speeding', {}).get('metrics', {}).get('travelled_segments', 0)
    total_new_road_segments_driven = results.get('speeding', {}).get('road_history_stats', {}).get('segments_not_driven_recently', 0)
    total_classified_types_road_segments = results.get('speeding', {}).get('road_types_travelled', {}).get('road_types_travelled_count', 0)
    total_night_driving_seconds = results.get('night_driving', {}).get('total_night_driving_seconds', 0)

    """
    1) Need to calculate total seconds for each event:
        - accel/decel: [differentiate type for Hard Braking and Rapid Acceleration]
        - cornering
        - distracted
        - speeding
        - night_driving (total_night_driving_seconds)
    2) Assign Penalty Values for each Event (need to make configurable later)
        Distracted: 26.0
        Speeding: 26.0
        Hard Braking: 600.0
        Rapid Acceleration: 600.0
        Hard Cornering: 1200.0
        Night Hours: 2.0
        Road Familiarity: 1.05
        Road Type: 1.05
    3) Create Multiplication Behavior Factors for each Event (need to make configurable later)
        Distracted:
            Speeding over 55 mph (check is_over_max_speed_allowed flag): 2.5
            Hard Braking: 2.0
            Night Hours: 3.0
        Speeding:
            Distracted: 2.5
            Hard Braking: 2.0
            Night Hours: 2.5
            Speeding in excess of 20 mph over limit(check is_severe_speeding flag): 2.5
        Hard Braking:
            Distracted: 2.0
            Speeding: 2.0
            Night Hours: 2.5
        Rapid Acceleration:
            Speeding: 1.50
            Night Hours: 3.0
        Cornering:
            Night Hours: 3.0
    4) Calculate total penalty for each behavior type (Penalty x Duration x Behavior Factor)
        - we will need to check overlapping events from event_pairing_count to determine if behavior factor applies
        - ex. total time for speeding was 20 seconds, we see in event_pairing_count that there
        was 1 overlap with distracted. So we apply the distracted factor of 2.5 to the speeding penalty calculation
        - the factor is static per overlap, so if there are 2 overlaps with distracted, we still apply the 2.5 factor once.
    5) Calculate behavior score for each type
        (Total Recorded Time - Total Penalty) / Total Recorded Time - for all except Road Familiarity and Type
            - total_seconds contains total recorded time
        (Total # of Segments - Total Penalty) / Total # of Segments - for Road Familiarity and Type
            - total_road_segments contains total number of road segments driven
    5.1) Calculate Night Driving Penalty
    5.2) Calculate Road Familiarity Penalty & Road Type Penalty
        (Total # of Segments - Total Penalty) / Total # of Segments = Penalty
        - Total Road Familiarity Penalty = road familiarity penalty x total_new_road_segments_driven
        - Total Road Type Penalty = road type penalty x total_classified_types_road_segments
    6) Apply behavior score to scoring weight %
        Distracted: 0.38
        Speeding: 0.25
        Hard Braking: 0.18
        Rapid Acceleration: 0.08
        Hard Cornering: 0.04
        Night Hours: 0.04
        Road Familiarity: 0.015
        Road Type: 0.015
    7) Sum weighted scores of each behavior to get final driving score for this trip
    """

    # --- Step 2: Aggregate total duration for each category ---

    total_durations = Counter()
    for event in all_events_detected:
        total_durations[(event["primary_event_category"], event["overlapping_event_category"])] += event.get('primary_event_duration', 0)
    
    print("--- Total Durations (seconds) ---\n")
    print(total_durations)
    print("\n---------------------------------\n")

    # --- Step 3: Base penalty values ---
    penalty_values = {
        'distracted': penalty_values['distracted'],
        'speeding': penalty_values['speeding'],
        'hard_braking': penalty_values['hard_braking'],
        'rapid_acceleration': penalty_values['rapid_acceleration'],
        'cornering': penalty_values['cornering'],
        'night_driving': penalty_values['night_driving'],
        'road_familiarity': penalty_values['road_familiarity'],
        'road_type': penalty_values['road_type'],
    }

    # --- Step 4: Multiplication behavior factors ---
    
    behavior_factors = {
        'distracted': behavior_factors['distracted'],
        'speeding': behavior_factors['speeding'],
        'hard_braking': behavior_factors['hard_braking'],
        'rapid_acceleration': behavior_factors['rapid_acceleration'],
        'cornering': behavior_factors['cornering']
    }

    # # --- Step 5: Calculate penalties for each behavior type ---
    valid_categories = {'distracted', 'speeding', 'hard_braking', 'rapid_acceleration', 'cornering'}
    total_penalties = {category: 0.0 for category in valid_categories}

    for (event_a, event_b), duration in total_durations.items():
        events = [event_a, event_b]

        # Iterate through both events in the pair
        for current_event in events:
            if current_event not in valid_categories or current_event is None:
                continue  # skip if not one of the tracked 5

            # Base penalty for this event
            base_penalty = penalty_values.get(current_event)
            if base_penalty is None:
                continue  # skip if no base penalty value

            # Determine the other event for factor lookup
            other_event = event_b if current_event == event_a else event_a

            # Default factor multiplier
            factor_multiplier = 1.0

            # Check if this event has a behavior factor with the other event
            if current_event in behavior_factors and other_event in behavior_factors[current_event]:
                factor_multiplier = behavior_factors[current_event][other_event]

            # Compute total penalty
            total_penalty = base_penalty * duration * factor_multiplier

            # Accumulate in total_penalties under the category
            total_penalties[current_event] += total_penalty


    # -- Step 5.1: Calculate night Driving Penalty ---
    total_penalties['night_driving'] = penalty_values.get('night_driving', 2.0) * total_night_driving_seconds

    # -- Step 5.2: Calculate Road Familiarity and Road Type Penalties ---
    total_penalties['road_type'] = penalty_values.get('road_type', 1.05) * total_classified_types_road_segments
    total_penalties['road_familiarity_penalty'] = penalty_values.get('road_familiarity', 1.05) * total_new_road_segments_driven

    # --- Step 6: Compute individual behavior scores ---
    behavior_scores = {}
    for category, penalty in total_penalties.items():
        if category in ['road_familiarity', 'road_type']:
            denominator = total_road_segments or 1
        else:
            denominator = total_seconds or 1
        behavior_scores[category] = round(max(0, (denominator - penalty) / denominator) * 100, 2)

    # --- Step 7: Weighted scoring ---
    weights = {
        'distracted': weights['distracted'],
        'speeding': weights['speeding'],
        'hard_braking': weights['hard_braking'],
        'rapid_acceleration': weights['rapid_acceleration'],
        'cornering': weights['cornering'],
        'night_driving': weights['night_driving'],
        'road_familiarity': weights['road_familiarity'],
        'road_type': weights['road_type']
    }

    weighted_sum = sum(behavior_scores.get(k, 1) * w for k, w in weights.items())
    final_score = round(weighted_sum, 2)

    # --- Step 9: Return detailed result breakdown ---
    scoring_breakdown = {
        'final_driving_score': final_score,
        'behavior_scores': behavior_scores,
        'total_durations': dict(total_durations),
        'total_penalties': total_penalties,
        'total_events_detected_count': len(all_events_detected)
    }

    print(scoring_breakdown)
    calculate_weighted_overall_driving_score(total_seconds, final_score, "./tests/user_driving_session_dummy2.json")
    print_star_ratings(scoring_breakdown)

    # return {
    #     'final_driving_score': final_score,
    #     'behavior_scores': behavior_scores,
    #     'total_durations': dict(total_durations),
    #     'total_penalties': total_penalties,
    #     'normalized_results_count': len(normalized_results)
    # }

def main():
    """
    Main entry point for detecting late night driving events.
    Can be run independently for testing purposes.
    """

if __name__ == "__main__":
    main()


