from decimal import Decimal, ROUND_HALF_UP
import json
import re

def extract_float(value: str) -> float:
    """
    Extracts the numeric value from a string and converts it to a float.
    
    :param value: A string containing a number with possible text.
    :return: The extracted number as a float.
    """
    match = re.search(r"\d+\.?\d*", value)
    return float(match.group()) if match else None

def write_dict_to_json(data: dict, filename: str, indent: int = 4) -> None:
    """
    Write a dictionary to a JSON file.

    Args:
        data (dict): The dictionary to write.
        filename (str): The path (and filename) of the JSON file to create or overwrite.
        indent (int, optional): Indentation level for pretty printing. Defaults to 4.

    Example:
        write_dict_to_json({"name": "Rey"}, "output.json")
    """
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=indent)
        print(f"✅ Successfully wrote data to {filename}")
    except Exception as e:
        print(f"❌ Error writing to {filename}: {e}")


def read_dict_from_json(filename: str) -> dict:
    """
    Read a dictionary from a JSON file.

    Args:
        filename (str): The path to the JSON file.

    Returns:
        dict: The contents of the JSON file as a dictionary.

    Example:
        data = read_dict_from_json("output.json")
    """
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"✅ Successfully read data from {filename}")
        return data
    except FileNotFoundError:
        print(f"❌ File not found: {filename}")
        return {}
    except json.JSONDecodeError as e:
        print(f"❌ Error decoding JSON from {filename}: {e}")
        return {}
    except Exception as e:
        print(f"❌ Error reading from {filename}: {e}")
        return {}

def convert_utc_offset_to_hours(utc_offset_str):
    sign = -1 if utc_offset_str.startswith('-') else 1
    hours, minutes, seconds = map(int, utc_offset_str.strip('+-').split(':'))
    offset_hours = sign * (hours + minutes/60 + seconds/3600)
    return offset_hours

def local_to_utc_hour(local_hour, offset_hours):
    utc_hour = (local_hour - offset_hours) % 24
    return int(utc_hour)

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

def convert_seconds(total_seconds: int) -> tuple[int, int]:
    """
    Converts a number of seconds into minutes and seconds.

    Args:
        total_seconds (int): The total number of seconds.

    Returns:
        tuple[int, int]: A tuple containing (minutes, seconds).
    """
    minutes = total_seconds // 60
    seconds = total_seconds % 60
    return minutes, seconds

def normalize_decimal(value):
    return Decimal(str(value)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
