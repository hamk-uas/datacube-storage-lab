import numpy as np
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
import collections
from pathlib import Path
from dask.distributed import Client
from dotenv import load_dotenv
load_dotenv()  # loads from .env in current directory

from .utils import band_groups

def get_random_patch_crs_coords(x1=300000, y1=6800040, x2=409800, y2=6690240, tile_size=(5100, 5100), resolution=(60, 60)):  # Defaults are 35VLH corners in UTM zone 35N CRS
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

def year_datacube_benchmark_safe(year, patch_crs_coords, folder="/scratch/project_2008694"):
    start = time.time()                   
    path_start = f'{folder}/safe/Sentinel-2/MSI/L1C/{year}'
    # Loop over months, in order
    patch_data_lists = {band_group: [] for band_group in band_groups.keys()}
    for month_folder in sorted((pathlib.Path(path_start)).glob('*')):
        month = month_folder.name
        print(f"Month {month}")
        # Loop over days, in order
        for day_folder in sorted(month_folder.glob('*')):
            day = day_folder.name
            print(f"Day {day}")
            # Loop over SAFEs
            for safe_folder in day_folder.glob('*.SAFE'):
                granule_folder = next((safe_folder / "GRANULE").glob('*'))
                # Loop over band groups
                for band_group in band_groups.keys():
                    #print(band_group)
                    bands = band_groups[band_group]["bands"]      
                    for band_index, band in enumerate(bands):
                        #print(band)
                        image_path = next((granule_folder / "IMG_DATA").glob(f'*{band}.jp2'))
                        #print(image_path)
                        ds = rioxarray.open_rasterio(image_path, chunks={"x": 1024, "y": 1024})
                        crs = ds.rio.crs
                        geo_transform = str_transform_to_transform(str(ds.rio.transform()))
                        upper_left_x, upper_left_y, lower_right_x, lower_right_y = get_patch_image_coords(geo_transform, *patch_crs_coords)
                        patch_data = ds.data[:, upper_left_y:lower_right_y, upper_left_x:lower_right_x]  # Uses Dask array slicing
                        patch_data_lists[band_group].append(patch_data)
        #break # !!! uncomment for 1 month test run
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

def year_datacube_benchmark_cog(year, patch_crs_coords, folder="/scratch/project_2008694"):
    start = time.time()
    path_start = f'{folder}/sentinel-s2-l1c-cogs/35/V/LH/{year}'
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

def year_datacube_benchmark_zarr(year, patch_crs_coords, zarr_folder = os.environ["DSLAB_S2L1C_NETWORK_ZARR_PATH"]):
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

def benchmark():
    client = Client()  # start distributed scheduler locally.
    benchmark_timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    with zarr.storage.LocalStore(f'{os.environ["DSLAB_S2L1C_NETWORK_ZARR_PATH"]}/sentinel-s2-l1c-zarr/', read_only=True) as zarr_store:
        tile = next(zarr_store.keys())
        year = next(zarr_store[tile].keys())
    storages = ["network", "local", "s3"]
    formats = collections.deque(["safe", "cog", "zarr"]) # A deque so that we can rotate the list easily for fairness
    total_durations = dict.fromkeys(formats, 0)
    durations = dict(((method, []) for method in formats))
    num_repeats = 10
    random.seed(42)
    for repeat in range(num_repeats):
        patch_crs_coords = get_random_patch_crs_coords()
        for format in formats:
            print(format)
            if format == "safe":
                duration, band_group_datacubes = year_datacube_benchmark_safe(year, patch_crs_coords, os.environ['LOCAL_SCRATCH'])
            elif format == "cog":
                duration, band_group_datacubes = year_datacube_benchmark_cog(year, patch_crs_coords, os.environ['LOCAL_SCRATCH'])
            elif format == "zarr":
                duration, band_group_datacubes = year_datacube_benchmark_zarr(year, patch_crs_coords, os.environ['LOCAL_SCRATCH'])
            total_durations[format] += duration
            durations[format].append(duration)
            for band_group, band_group_datacube in band_group_datacubes.items():
                print(band_group, band_group_datacube.shape)
        formats.rotate(1)
        # Serializing json
        with open(Path(os.environ[""]) / f"sentinel2_l1c_{benchmark_timestamp}.json", "w") as out_file:
            json.dump({
                "tile": tile,
                "year": year,
                "durations": durations,
                "total_durations": total_durations
            }, out_file, indent = 4)    

if __name__=="__main__":
    benchmark()
