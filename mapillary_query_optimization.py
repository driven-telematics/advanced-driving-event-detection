import argparse
import time
import mercantile
from geopy.distance import geodesic
import asyncio
import aiohttp

API_TOKEN = ""
zoom = 18

def tile_area(bounds):
    """Compute area of a tile bounds in square degrees."""
    return abs((bounds.north - bounds.south) * (bounds.east - bounds.west))

def cluster_tiles(tiles, cluster_size):
    """Group tiles into clusters of N tiles and return combined BBoxes; Keep less than 4"""
    for i in range(0, len(tiles), cluster_size):
        chunk = tiles[i:i + cluster_size]
        bounds = [mercantile.bounds(t.x, t.y, t.z) for t in chunk]

        south = min(b.south for b in bounds)
        west = min(b.west for b in bounds)
        north = max(b.north for b in bounds)
        east = max(b.east for b in bounds)

        yield (south, west, north, east)

def parse_mapillary_speed_limit(object_value):
    """Extract speed from `regulatory--maximum-speed-limit-XX` string."""
    parts = object_value.split("-")
    if len(parts) < 4:
        return None
    try:
        return float(parts[-3])
    except ValueError:
        return None

# ----------------------------------------------
# Mapillary API (cached)
# ----------------------------------------------

tile_cache = {}

async def get_mapillary_speed_limits_cached(session, south, west, north, east):
    """Cached Mapillary API call."""
    key = (round(south, 5), round(west, 5), round(north, 5), round(east, 5))

    if key in tile_cache:
        return tile_cache[key]

    url = (
        "https://graph.mapillary.com/map_features"
        f"?access_token={API_TOKEN}"
        f"&fields=id,object_value,geometry"
        f"&layers=trafficsigns"
        f"&bbox={west},{south},{east},{north}"
    )

    try:
        async with session.get(url) as response:
            if response.status != 200:
                text = await response.text()
                print(f"Mapillary Error {response.status}: {text}")
                tile_cache[key] = []
                return []

            json_data = await response.json()
            data = json_data.get("data", [])

            # Filter only maximum-speed-limit signs
            filtered = [
                item for item in data
                if "regulatory--maximum-speed-limit" in item.get("object_value", "")
            ]
            tile_cache[key] = filtered
            return filtered

    except Exception as e:
        print(f"Request failed: {e}")
        tile_cache[key] = []
        return []

async def get_all_tiles_async(clusters):
    """
    Fetch all tiles concurrently using asyncio
    """
    async with aiohttp.ClientSession() as session:
        tasks = [
            get_mapillary_speed_limits_cached(session, *cluster)
            for cluster in clusters
        ]

        # Run all fetches in parallel
        results = await asyncio.gather(*tasks)
        combined = []
        for r in results:
            combined.extend(r)

        return combined


def process_data_file(input_file, cluster_size):
    # Load points
    with open(input_file, "r") as file:
        raw = file.read().strip().split("|")
    
    points = []
    for row in raw:
        parts = row.split(",")

        lat = float(parts[0])
        lon = float(parts[1])
        ts = int(parts[4])  

        points.append((lat, lon, ts))


    total_distance = 0.0
    start_time = points[0][2]
    end_time = points[0][2]

    prev_lat, prev_lon, _ = points[0]

    for lat, lon, ts in points[1:]:
        total_distance += geodesic((prev_lat, prev_lon), (lat, lon)).miles
        end_time = ts
        prev_lat, prev_lon = lat, lon

    total_seconds = end_time - start_time
    minutes = total_seconds // 60
    seconds = total_seconds % 60


    lats = [p[0] for p in points]
    lons = [p[1] for p in points]

    lat_min, lat_max = min(lats), max(lats)
    lon_min, lon_max = min(lons), max(lons)

    print("Session BBox:")
    print("  South:", lat_min)
    print("  North:", lat_max)
    print("  West:", lon_min)
    print("  East:", lon_max)

    # ----------------------------------------------
    # 1. Generate tiles
    # ----------------------------------------------
    tiles = list(mercantile.tiles(lon_min, lat_min, lon_max, lat_max, zoom))
    print(f"Total tiles: {len(tiles)}")

    # ----------------------------------------------
    # 2. Cluster tiles to reduce API calls (BBoxes)
    # ----------------------------------------------
    clusters = list(cluster_tiles(tiles, cluster_size))
    print(f"Tiles per cluster/API call: {cluster_size}")
    print(f"Total tile clusters (API calls): {len(clusters)}")

    # ----------------------------------------------
    # 3. Query Mapillary
    # ----------------------------------------------
    # Run async tile fetching
    start = time.time()
    results = asyncio.run(get_all_tiles_async(clusters))
    elapsed = time.time() - start


    print("\n-------- RESULTS --------")
    print(f"Total Speed Signs: {len(results)}")
    # print(f"Speed Signs: {results}")
    print(f"Total API Calls: {len(clusters)} (in parallel)")
    print(f"Time: {elapsed:.2f} seconds")
    print(f"Total Distance: {total_distance:.2f} miles")
    print(f"Total Duration: {minutes} minutes {seconds} seconds")
    print(f"{len(clusters)} Calls | {elapsed:.2f} s | {len(results)} signs")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process Mapillary data.")
    parser.add_argument("input_file", type=str, help="Path to the input data file.")
    parser.add_argument("--cluster_size", type=int, default=10, help="Number of tiles per cluster (default: 10).")

    args = parser.parse_args()
    process_data_file(args.input_file, args.cluster_size)