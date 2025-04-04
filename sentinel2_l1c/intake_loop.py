from datetime import datetime, timedelta
import pytz

from sentinel2_download_safe import download_safe

# Set start and end dates
start_date = datetime(2024, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)
end_date = datetime(2025, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)
tile_id = "35VLH"

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