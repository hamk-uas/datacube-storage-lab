import os
import json
import subprocess
import argparse
import uuid
import shutil
from datetime import datetime

import pystac_client
import boto3
from tenacity import retry, stop_after_attempt, wait_fixed
from osgeo import gdal

s3_session = boto3.Session(profile_name='cdse')
s3_resource = s3_session.resource('s3')
s3_client = s3_session.client('s3')

@retry(wait=wait_fixed(2), stop=stop_after_attempt(100))
def get_stac_response(start, end, tile_id):
    stac_io = pystac_client.stac_api_io.StacApiIO()
    response = stac_io.request(
        "https://catalogue.dataspace.copernicus.eu/stac/search",
        method='POST',
        parameters={
            "filter_lang": "cql2-json",
            "filter":{
                "op": "and",
                "args": [
                    {
                        "op": "=",
                        "args": [
                            {
                                "property": "collection"
                            },
                            "SENTINEL-2"
                        ]
                    },
                    {
                        "op": "=",
                        "args": [
                            {
                                "property": "processingLevel"
                            },
                            "S2MSI1C"
                        ]
                    },
                    {
                        "op": "=", 
                        "args": [
                            {
                                "property": "tileId"
                            },
                            tile_id
                        ]
                    },
                    {
                        "op": "t_intersects",
                        "args": [
                            {
                                "property": "datetime"
                            },
                            {
                                "interval": [
                                    start,
                                    end
                                ]
                            }
                        ]
                    }
                ]
            },
            "limit": 1000
        }
    )
    print(response)
    return json.loads(response)

def download_safe_items(items, safe_folder="./safe"):
    os.makedirs(safe_folder, exist_ok=True)
    for item_index, item in enumerate(items):
        s3_product = item["assets"]["PRODUCT"]["alternate"]["s3"]["href"]
        print(s3_product)
        bucket_name, product = s3_product.strip("/").split("/", 1)
        bucket = s3_resource.Bucket(bucket_name)
        objects = bucket.objects.filter(Prefix=product)
        for key in [object.key for object in objects]:
            local_file_name = os.path.join(safe_folder, key)
            os.makedirs(os.path.split(local_file_name)[0], exist_ok=True)
            bucket.download_file(key, local_file_name)

def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Download images for a Sentinel2 L1C SAFE tile within the given time range'
    )

    defaults = {
        "time_start": '2024-02-21T00:00:00Z',
        "time_end": '2024-02-22T00:00:00Z',
        "tile_id": "35VLH"
    }
    parser.add_argument(
        '--time_start',
        type=str,
        default=defaults["time_start"],
        help=f'Start time in UTC format (YYYY-MM-DDTHH:MM:SSZ), default: {defaults["time_start"]}'
    )

    parser.add_argument(
        '--time_end',
        type=str,
        default=defaults["time_end"],
        help=f'End time in UTC format (YYYY-MM-DDTHH:MM:SSZ), default: {defaults["time_end"]}'
    )

    parser.add_argument(
        '--tile_id',
        type=str,
        default=defaults["tile_id"],
        help=f'Tile identifier string, default: {defaults["tile_id"]}'
    )

    return parser.parse_args()

def download_safe(tile_id, time_start, time_end):
    try:
        start_time = datetime.strptime(time_start, '%Y-%m-%dT%H:%M:%SZ')
        end_time = datetime.strptime(time_end, '%Y-%m-%dT%H:%M:%SZ')

        if end_time <= start_time:
            raise ValueError("time_end must be after time_start")

        print(f"Start Time: {time_start}")
        print(f"End Time: {time_end}")
        print(f"Tile ID: {tile_id}")

    except ValueError as e:
        print(f"Error: {e}")
        print("Time format should be YYYY-MM-DDTHH:MM:SSZ (e.g., 2025-02-20T12:00:00Z)")

    json_response = get_stac_response(time_start, time_end, tile_id)
    items = json_response["features"]
    download_safe_items(items)

if __name__ == "__main__":
    args = parse_arguments()
    download_safe(**vars(args))