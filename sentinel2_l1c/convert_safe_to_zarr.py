import os
import rasterio
import xarray as xr
import numpy as np
import zarr
from pathlib import Path
import time

if int(zarr.__version__.split(".")[0]) < 3:
    raise ImportError("zarr version 3 or higher is required. Current version: {zarr.__version__}")

from .utils import band_groups

def convert(safe_from_folder = os.environ["DSLAB_S2L1C_NETWORK_SAFE_PATH"], zarr_to_folder = os.environ["DSLAB_S2L1C_NETWORK_ZARR_PATH"]):
    start = time.time()              
    total_num_items = 0

    zarr_store = zarr.storage.LocalStore(zarr_to_folder + "/", read_only=False)

    # Loop over years, in order
    for year_folder in sorted((Path(safe_from_folder) / f"Sentinel-2/MSI/L1C").glob('*')):
        year = year_folder.name
        # Loop over months, in order
        for month_folder in sorted(year_folder.glob('*')):
            month = month_folder.name
            # Loop over days, in order
            for day_folder in sorted(month_folder.glob('*')):
                day = day_folder.name
                # Loop over SAFEs
                for safe_folder in day_folder.glob('*.SAFE'):
                    total_num_items += 1
                    safe_name = safe_folder.name
                    print("safe_name", safe_name)
                    tile_id = safe_name.split(sep="_")[5][1:]  # For example "35VLH"
                    tile_id_zarrgroup = zarr.group(store=zarr_store, path=f"{tile_id}")
                    year_zarrgroup = tile_id_zarrgroup.require_group(f"{year}")
                    safe_time = safe_name.split(sep="_")[2]
                    utc_time = f"{safe_time[:4]}-{safe_time[4:6]}-{safe_time[6:8]}T{safe_time[9:11]}:{safe_time[11:13]}:{safe_time[13:]}Z"
                    print(utc_time)  # For example "2024-02-16T09:50:29Z"
                    granule_folder = next((safe_folder / "GRANULE").glob('*'))
                    # Loop over band groups
                    for band_group, band_group_dict in band_groups.items():
                        print("Band group:", band_group)
                        bands = band_group_dict["bands"]
                        # Check whether we will create, append or skip (in case the time is already in Zarr)
                        band_group_zarrgroup = year_zarrgroup.require_group(band_group)
                        print(f"Checking if Zarr exists (group {band_group_zarrgroup.path})")
                        old_zarr_ds = xr.open_zarr(zarr_store, group=band_group_zarrgroup.path, consolidated=False)
                        if len(old_zarr_ds.variables) == 0:
                            print("Zarr does not exist. Create it.")
                            action = "create"
                        elif utc_time in old_zarr_ds.coords["time"].values:
                            print("Zarr exists and already contains this time. Do nothing (maybe this was a conversion rerun).")
                            action = "skip"
                        else:
                            print("Zarr exists and doesn't contain this time. Append data for this time to Zarr.")
                            action = "append"
                        old_zarr_ds.close()
                        del old_zarr_ds
                        if action == "skip":
                            # Skip
                            continue
                        band_data_array = []
                        # Loop over bands
                        for band_index, band in enumerate(bands):
                            for img_index, img_path in enumerate((granule_folder / "IMG_DATA").glob(f'*{band}.jp2')):  # There is only one image for the band, but use a loop anyhow
                                print(img_path)
                                with rasterio.open(img_path) as src:
                                    data = src.read(1)
                                    band_data_array.append(data)# = (["y", "x"], data)
                                    if band_index == 0 and img_index == 0:
                                        transform = src.transform
                                        crs = src.crs
                                        height, width = src.height, src.width
                        band_data_array = np.expand_dims(np.stack(band_data_array, axis=0), axis=0)
                        ds = xr.Dataset(
                            {"data": (["time", "band", "y", "x"], band_data_array)},
                            coords={"time": [utc_time], "band": bands, "y": np.arange(height), "x": np.arange(width)},
                            attrs={"crs": str(crs), "transform": str(transform)}
                        )
                        if action == "create":
                            # Create
                            compressor = zarr.codecs.BloscCodec(cname="lz4", clevel=5, shuffle=zarr.codecs.BloscShuffle('bitshuffle'))
                            encoding = {
                                "data": {
                                    "compressors": compressor,
                                    "chunks": (band_group_dict["time_chunk_size"], len(bands), band_group_dict["y_chunk_size"], band_group_dict["x_chunk_size"]),  # (Time, band, Y, X) chunk sizes
                                }
                            }
                            print(encoding)
                            ds.to_zarr(zarr_store, mode="w", group=band_group_zarrgroup.path, zarr_format=3, encoding=encoding, consolidated=False)
                        else:
                            # Append
                            ds.to_zarr(zarr_store, group=band_group_zarrgroup.path, append_dim="time", zarr_format=3, consolidated=False)

    duration = time.time() - start
    print("Duration (s):", duration)
    print("Total number of SAFE items:", total_num_items)
                            
if __name__ == "__main__":
    convert()