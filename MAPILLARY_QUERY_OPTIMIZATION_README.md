# Mapillary Query Optimization

This script processes geospatial data to query the Mapillary API for traffic signs, specifically speed limit signs, within a given bounding box. It clusters map tiles to optimize API calls and fetches data asynchronously for better performance.

## Features

- **Tile Clustering**: Groups map tiles into clusters to reduce the number of API calls.
- **Asynchronous API Calls**: Uses `asyncio` and `aiohttp` to fetch data concurrently.
- **Distance and Duration Calculation**: Computes the total distance traveled and the duration of the session based on input data.
- **Bounding Box Calculation**: Determines the bounding box of the session from the input data.
- **Speed Limit Sign Extraction**: Filters Mapillary API results to include only speed limit signs.

## Requirements

- Python 3.7 or higher
- Dependencies:
  - `aiohttp`
  - `mercantile`
  - `geopy`

## Arguments

- input_file: Path to the input data file. The file should contain rows of data in the format: latitude,longitude,unused,unused,timestamp,... separated by |.
- cluster_size: (Optional) Number of tiles per cluster. Default is 10.

## Output/Outcome
- At the moment, this script was used to test ways to optimize querying Mapillary.
- The goal was to find a better way to efficiently query Mapillary and then integrate it into the speeding event detection service. However, after testing we realized that this will not be sustainable for scale.

### Install the required dependencies using pip:

```bash
pip install aiohttp mercantile geopy