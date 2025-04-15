import numpy as np
import boto3
import rasterio
import time
import zarr
import pathlib
import random
import os
import xarray as xr
import dask
import re
import rioxarray
import json
import datetime
import argparse
from pathlib import Path
import xmltodict
from dotenv import load_dotenv
load_dotenv()  # loads from .env in current directory

from .utils import band_groups

def get_safe_bounding_cube():
    # Get a SAFE (any SAFE!)
    safe_folder = os.environ["DSLAB_S2L1C_NETWORK_SAFE_PATH"]
    year_folder = sorted((Path(safe_folder) / f"Sentinel-2/MSI/L1C").glob('*'))[0]
    year = year_folder.name
    month_folder = sorted(year_folder.glob('*'))[0]
    month = month_folder.name
    day_folder = sorted(month_folder.glob('*'))[0]
    day = day_folder.name
    safe_folder = next(day_folder.glob('*.SAFE'))
    safe_name = safe_folder.name
    tile = safe_name.split(sep="_")[5][1:]  # For example "35VLH"
    granule_folder = next((safe_folder / "GRANULE").glob('*'))
    granule_metadata_file_name = os.path.join(granule_folder, "MTD_TL.xml")
    with open(granule_metadata_file_name, "r", encoding="utf-8") as file:
        xml_content = file.read()
    granule_metadata_dict = xmltodict.parse(xml_content)
    x1 = int(granule_metadata_dict["n1:Level-1C_Tile_ID"]['n1:Geometric_Info']['Tile_Geocoding']['Geoposition'][0]['ULX'])
    y1 = int(granule_metadata_dict["n1:Level-1C_Tile_ID"]['n1:Geometric_Info']['Tile_Geocoding']['Geoposition'][0]['ULY'])
    x2 = x1 + 109800
    y2 = y1 - 109800
    return {
        "tile": tile,
        "year": year,
        "x1": x1,
        "y1": y1,
        "x2": x2,
        "y2": y2
    }

def get_random_patch_crs_coords(x1, y1, x2, y2, tile_size=(5100, 5100), resolution=(60, 60)):  # Defaults are 35VLH corners in UTM zone 35N CRS
    patch_x1 = x1 + random.randint(0, ((x2 - x1) - tile_size[0])//resolution[0])*resolution[0]  # We assume x1 is origin and x1 < x2
    patch_y1 = y1 - random.randint(0, ((y1 - y2) - tile_size[1])//resolution[1])*resolution[1]  # We assume y1 is origin and y2 < y1
    patch_x2 = patch_x1 + tile_size[0]
    patch_y2 = patch_y1 - tile_size[1]
    return patch_x1, patch_y1, patch_x2, patch_y2
    
def get_patch_image_coords(transform, patch_x1, patch_y1, patch_x2, patch_y2):
    upper_left_x, upper_left_y = crs_coords_to_image_coords(transform, patch_x1, patch_y1)
    lower_right_x, lower_right_y = crs_coords_to_image_coords(transform, patch_x2, patch_y2)
    return upper_left_x, upper_left_y, lower_right_x, lower_right_y

# Define a function to convert geographic coordinates to pixel coordinates.
def crs_coords_to_image_coords(transform, crs_x, crs_y):
    # Extract the geographic transformation parameters.
    ul_x = transform[0]  # x of the upper left corner
    ul_y = transform[3]  # y of the upper left corner
    x_res = transform[1]  # x resolution (pixel size in x)
    y_res = transform[5]  # y resolution (pixel size in y)    
    # Convert geographic coordinates to pixel coordinates.
    pixel_x = int((crs_x - ul_x) / x_res)
    pixel_y = int((crs_y - ul_y) / y_res)
    return pixel_x, pixel_y

def str_transform_to_transform(matrix_string):
    #| 60.00, 0.00, 300000.00|
    #| 0.00,-60.00, 6800040.00|
    #| 0.00, 0.00, 1.00|
    # will convert to:
    #(300000.0, 60.0, 0.0, 6800040.0, 0.0, -60.0)

    # Remove unwanted characters and split into rows
    rows = matrix_string.strip().split('\n')
    # Extract numerical values from each row
    matrix = [list(map(float, re.findall(r'-?\d+\.\d+', row))) for row in rows]
    return matrix[0][2], matrix[0][0], matrix[0][1], matrix[1][2], matrix[1][0], matrix[1][1]

def year_datacube_benchmark_safe(year, patch_crs_coords, folder, storage="filesystem", s3_endpoint=None, s3_bucket=None):
    start = time.time()                   
    path_start = f'{folder}/Sentinel-2/MSI/L1C/{year}'
    if storage == "s3":
        s3_path_start = f"/vsicurl/{s3_endpoint}/{s3_bucket}/{year}"
    # Loop over months, in order
    patch_data_lists = {band_group: [] for band_group in band_groups.keys()}
    for month_folder in sorted((pathlib.Path(path_start)).glob('*')):
        month = month_folder.name
        if storage == "s3":
            month_s3_path = f"{s3_path_start}/{month}"
        print(f"Month {month}")
        # Loop over days, in order
        for day_folder in sorted(month_folder.glob('*')):
            day = day_folder.name
            if storage == "s3":
                day_s3_path = f"{month_s3_path}/{day}"
            print(f"Day {day}")
            # Loop over SAFEs
            for safe_folder in day_folder.glob('*.SAFE'):
                safe_name = safe_folder.name
                granule_folder = next((safe_folder / "GRANULE").glob('*'))
                granule = granule_folder.name
                if storage == "s3":
                    safe_s3_path = f"{day_s3_path}/{safe_name}"
                    granule_s3_path = f"{safe_s3_path}/GRANULE/{granule}"
                # Loop over band groups
                for band_group in band_groups.keys():
                    #print(band_group)
                    bands = band_groups[band_group]["bands"]      
                    for band in bands:
                        #print(band)
                        image_path = next((granule_folder / "IMG_DATA").glob(f'*{band}.jp2'))
                        if storage == "s3":
                            image_s3_path = f"{granule_s3_path}/IMG_DATA/{image_path.name}"
                        #print(image_path)
                        if storage == "filesystem":
                            use_image_path = image_path
                        elif storage == "s3":
                            use_image_path = image_s3_path
                        ds = rioxarray.open_rasterio(use_image_path, chunks={"x": 1024, "y": 1024})
                        #crs = ds.rio.crs
                        geo_transform = str_transform_to_transform(str(ds.rio.transform()))
                        upper_left_x, upper_left_y, lower_right_x, lower_right_y = get_patch_image_coords(geo_transform, *patch_crs_coords)
                        patch_data = ds.data[:, upper_left_y:lower_right_y, upper_left_x:lower_right_x]  # Uses Dask array slicing
                        patch_data_lists[band_group].append(patch_data)
        break # !!! uncomment for 1 month test run
    band_group_datacubes = {}
    for band_group in band_groups:
        dask_stack = dask.array.stack(patch_data_lists[band_group], axis=0)
        band_group_datacubes[band_group] = dask_stack.compute()
        shape = band_group_datacubes[band_group].shape
        band_group_datacubes[band_group] = band_group_datacubes[band_group].reshape(
            (shape[0]//len(band_groups[band_group]["bands"]), len(band_groups[band_group]["bands"]), shape[2], shape[3])
        )
    duration = time.time() - start
    return duration, band_group_datacubes

def year_datacube_benchmark_cog(year, patch_crs_coords, folder, storage="filesystem", s3_endpoint=None, s3_bucket=None):
    start = time.time()
    path_start = f'{folder}/sentinel-s2-l1c-cog/35/V/LH/{year}'
    # Loop over months, in order
    patch_data_lists = {band_group: [] for band_group in band_groups.keys()}
    for month_folder in [pathlib.Path(path_start) / str(month) for month in sorted([int(month.name) for month in (pathlib.Path(path_start)).glob('*')])]:
        month = month_folder.name
        for cog_folder_path in month_folder.glob('*'):
            for band_group in band_groups.keys():
                band_group_image_path = cog_folder_path / f"{band_group}.tif"
                ds = rioxarray.open_rasterio(band_group_image_path, chunks={"x": 512, "y": 512}, engine="rasterio")
                crs = ds.rio.crs
                geo_transform = str_transform_to_transform(str(ds.rio.transform()))
                upper_left_x, upper_left_y, lower_right_x, lower_right_y = get_patch_image_coords(geo_transform, *patch_crs_coords)
                patch_data = ds.data[:, upper_left_y:lower_right_y, upper_left_x:lower_right_x]  # Uses Dask array slicing
                patch_data_lists[band_group].append(patch_data)
    band_group_datacubes = {}
    for band_group in band_groups:
        dask_stack = dask.array.stack(patch_data_lists[band_group], axis=0)
        band_group_datacubes[band_group] = dask_stack.compute()
    
    duration = time.time() - start
    return duration, band_group_datacubes

def year_datacube_benchmark_zarr(year, patch_crs_coords, folder, storage="filesystem", s3_endpoint=None, s3_bucket=None):
    start = time.time()
    band_group_datacubes = {}
    for band_group in band_groups.keys():
        band_group_datacubes[band_group] = np.zeros((
            0,
            len(band_groups[band_group]["bands"]),
            abs(patch_crs_coords[3]-patch_crs_coords[1])//band_groups[band_group]["resolution"],
            abs(patch_crs_coords[2]-patch_crs_coords[0])//band_groups[band_group]["resolution"]
        ))
    zarr_store = zarr.storage.LocalStore(f'{folder}/sentinel-s2-l1c-zarr/', read_only=True)
    zarr_root = zarr.open(zarr_store, mode='r')
    tile_zarr_group = zarr_root["35VLH"]
    year_zarr_group = tile_zarr_group[f"{year}"]
    for band_group in band_groups.keys():
        print(band_group)
        band_zarr_group = year_zarr_group[band_group]
        ds = xr.open_zarr(zarr_store, band_zarr_group.path, zarr_format=3, chunks={})
        crs = ds.attrs.get("crs", None)
        geo_transform = str_transform_to_transform(ds.attrs.get("transform", None))
        upper_left_x, upper_left_y, lower_right_x, lower_right_y = get_patch_image_coords(geo_transform, *patch_crs_coords)
        patch_data = ds['data'].data[:, :, upper_left_y:lower_right_y, upper_left_x:lower_right_x]  # Uses Dask array slicing
        band_group_datacubes[band_group] = patch_data.compute()  # Only computes required part
    duration = time.time() - start
    return duration, band_group_datacubes

def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Benchmark patch timeseries loading from multiple storage systems and formats'
    )

    bounding_cube = get_safe_bounding_cube()
    #with zarr.storage.LocalStore(f'{os.environ["DSLAB_S2L1C_NETWORK_ZARR_PATH"]}/sentinel-s2-l1c-zarr/', read_only=True) as zarr_store:
    #    tile = next(zarr_store.keys())
    #    year = next(zarr_store[tile].keys())

    defaults = {
        "storages": ["network", "temp", "s3"],
        "formats": ["safe", "cog", "zarr"],
        "num_repeats": 10,
        "year": bounding_cube["year"],
        "tile": bounding_cube["tile"],
        "x1": bounding_cube["x1"],
        "y1": bounding_cube["y1"],
        "x2": bounding_cube["x2"],
        "y2": bounding_cube["y2"]
    }

    parser.add_argument(
        '--storages',
        type=str,
        nargs='+',
        choices=["network", "temp", "s3"],
        default=defaults["storages"],
        help=f'List of space-separated ids of storages to benchmark, default: {" ".join(defaults["storages"])}'
    )

    parser.add_argument(
        '--formats',
        type=str,
        nargs='+',
        choices=["safe", "cog", "zarr"],
        default=defaults["formats"],
        help=f'List of space-separated ids of formats to benchmark, default: {" ".join(defaults["formats"])}'
    )
    
    parser.add_argument(
        '--num_repeats',
        type=int,
        default=defaults["num_repeats"],
        help=f'Number of repeats, default: {defaults["num_repeats"]}'
    )

    parser.add_argument(
        '--year',
        type=int,
        default=defaults["year"],
        help=f'Year, default (from network SAFE): {defaults["year"]}'
    )

    parser.add_argument(
        '--tile',
        type=str,
        default=defaults["tile"],
        help=f'Tile, default (from network SAFE): {defaults["tile"]}'
    )

    parser.add_argument(
        '--x1',
        type=int,
        default=defaults["x1"],
        help=f'Bounding box x1, default (from network SAFE): {defaults["x1"]}'
    )
    parser.add_argument(
        '--y1',
        type=int,
        default=defaults["y1"],
        help=f'Bounding box y1, default (from network SAFE): {defaults["y1"]}'
    )
    parser.add_argument(
        '--x2',
        type=int,
        default=defaults["x2"],
        help=f'Bounding box x2, default (from network SAFE): {defaults["x2"]}'
    )
    parser.add_argument(
        '--y2',
        type=int,
        default=defaults["y2"],
        help=f'Bounding box y2, default (from network SAFE): {defaults["y2"]}'
    )

    return parser.parse_args()

def benchmark(storages, formats, num_repeats, year, tile, x1, y1, x2, y2):
    if "s3" in storages:
        s3_session = boto3.Session(profile_name=os.environ["DSLAB_S2L1C_S3_PROFILE"])
        s3_client = s3_session.client('s3')
        s3_endpoint_url = s3_client.meta.endpoint_url    
    benchmark_timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    print(f"Benchmarking loading data for tile {tile}, year {year}")
    log = {
        "tile": tile,
        "year": year,
        "results": {

        }
    }
    for storage in storages:
        log["results"][storage] = {}
        for format in formats:
            log["results"][storage][format] = {
                "durations": [],
                "band_group_shapes": {},
                "total_duration": 0,
            }
    num_repeats = 10
    random.seed(42)
    for repeat in range(num_repeats + 1):
        random.shuffle(storages)
        random.shuffle(formats)
        patch_crs_coords = get_random_patch_crs_coords(x1=x1, y1=y1, x2=x2, y2=y2)
        for storage in storages:
            for format in formats:
                print(format)
                if format == "safe":
                    if (storage == "temp"):
                        duration, band_group_datacubes = year_datacube_benchmark_safe(year, patch_crs_coords, folder=os.environ["DSLAB_S2L1C_TEMP_SAFE_PATH"])
                    elif (storage == "network"):
                        duration, band_group_datacubes = year_datacube_benchmark_safe(year, patch_crs_coords, folder=os.environ["DSLAB_S2L1C_NETWORK_SAFE_PATH"])
                    elif (storage == "s3"):
                        duration, band_group_datacubes = year_datacube_benchmark_safe(year, patch_crs_coords, storage="s3", s3_endpoint_url=s3_endpoint_url, s3_bucket=os.environ["DSLAB_S2L1C_S3_SAFE_BUCKET"])
                elif format == "cog":
                    if (storage == "temp"):
                        duration, band_group_datacubes = year_datacube_benchmark_cog(year, patch_crs_coords, folder=os.environ["DSLAB_S2L1C_TEMP_COG_PATH"])
                    elif (storage == "network"):
                        duration, band_group_datacubes = year_datacube_benchmark_cog(year, patch_crs_coords, folder=os.environ["DSLAB_S2L1C_NETWORK_COG_PATH"])
                    elif (storage == "s3"):
                        duration, band_group_datacubes = year_datacube_benchmark_cog(year, patch_crs_coords, storage="s3", s3_endpoint_url=s3_endpoint_url, s3_bucket=os.environ["DSLAB_S2L1C_S3_COG_BUCKET"])
                elif format == "zarr":
                    if (storage == "temp"):
                        duration, band_group_datacubes = year_datacube_benchmark_zarr(year, patch_crs_coords, folder=os.environ["DSLAB_S2L1C_TEMP_ZARR_PATH"])
                    elif (storage == "network"):
                        duration, band_group_datacubes = year_datacube_benchmark_zarr(year, patch_crs_coords, folder=os.environ["DSLAB_S2L1C_NETWORK_ZARR_PATH"])
                    elif (storage == "s3"):
                        duration, band_group_datacubes = year_datacube_benchmark_zarr(year, patch_crs_coords, storage="s3", s3_endpoint_url=s3_endpoint_url, s3_bucket=os.environ["DSLAB_S2L1C_S3_ZARR_BUCKET"])
                if repeat > 0:
                    log["results"][storage][format]["total_duration"] += duration
                    log["results"][storage][format]["durations"].append(duration)
                    log["results"][storage][format]["mean_durations"] = np.mean(log["results"][storage][format]["durations"])
                    log["results"][storage][format]["std_durations"] = np.std(log["results"][storage][format]["durations"])
                    log["results"][storage][format]["stderr_durations"] = np.std(log["results"][storage][format]["durations"]) / len(log["results"][storage][format]["durations"])           
                    log["results"][storage][format]["band_group_shapes"] = {}
                    for band_group, band_group_datacube in band_group_datacubes.items():
                        log["results"][storage][format]["band_group_shapes"][band_group] = band_group_datacube.shape
        if repeat > 0:
            # Serializing json
            logpath = Path(os.environ["DSLAB_LOG_FOLDER"]) / f"sentinel2_l1c_{benchmark_timestamp}.json"
            print(f"Writing log to: {logpath}")
            logpath.parent.mkdir(parents=True, exist_ok=True)
            with open(logpath, "w") as out_file:
                json.dump(log, out_file, indent = 4)    

if __name__=="__main__":
    args = parse_arguments()
    benchmark(**vars(args))
