from detect_all_driving_events import convert_timestamp

def event_time_overlap(event1, event2):
    """
    Returns True if event1 and event2 time windows overlap.
    event1/event2: dicts with 'start_time' and 'end_time' (unix timestamps)
    """

    return not (event1['end_time'] < event2['start_time'] or event2['end_time'] < event1['start_time'])