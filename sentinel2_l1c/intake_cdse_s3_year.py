from datetime import datetime, timedelta
import pytz
import argparse

from .intake_cdse_s3 import download_safe

def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Download images for a Sentinel2 L1C SAFE tile within the given UTC year range'
    )

    defaults = {
        "year_start": 2024, # -01-01T00:00:00Z
        "year_end": 2025, # -01-01T00:00:00Z
        "tile_id": "35VLH"
    }
    parser.add_argument(
        '--year_start',
        type=int,
        default=defaults["year_start"],
        help=f'Start year, default: {defaults["year_start"]}'
    )

    parser.add_argument(
        '--year_end',
        type=int,
        default=defaults["year_end"],
        help=f'End year (not included), default: {defaults["year_end"]}'
    )

    parser.add_argument(
        '--tile_id',
        type=str,
        default=defaults["tile_id"],
        help=f'Tile identifier string, default: {defaults["tile_id"]}'
    )

def download_safe_years(tile_id, year_start, year_end):
    # Set start and end dates
    start_date = datetime(year_start, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)
    end_date = datetime(year_end, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)

    # Date starts at start_date
    date = start_date

    utc_midnights = []
    # Loop through all days
    while date <= end_date:
        # Format the datetime as ISO 8601 UTC string
        utc_string = date.strftime('%Y-%m-%dT%H:%M:%SZ')
        utc_midnights.append(utc_string)
        
        # Increment by one day
        date += timedelta(days=1)

    for day in range(len(utc_midnights) - 1):
        download_safe(tile_id=tile_id, time_start=utc_midnights[day], time_end=utc_midnights[day + 1])

if __name__ == "__main__":
    args = parse_arguments()
    download_safe_years(**vars(args))        