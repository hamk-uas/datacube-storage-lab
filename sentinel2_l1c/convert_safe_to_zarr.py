import os
import warnings
import rasterio
import xarray as xr
import numpy as np
import zarr
from pathlib import Path

from .utils import band_groups

def convert(safe_from_folder = os.environ["DSLAB_S2L1C_NETWORK_SAFE_PATH"], zarr_to_folder = os.environ["DSLAB_S2L1C_NETWORK_ZARR_PATH"]):

    zarr_store = zarr.storage.LocalStore(zarr_to_folder, read_only=False)

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
                    safe_name = safe_folder.name
                    tile_id = safe_name.split(sep="_")[5][1:]  # For example "35VLH"
                    tile_id_zarr_group = zarr.group(store=zarr_store, path=f"{tile_id}")
                    year_zarr_group = tile_id_zarr_group.require_group(f"{year}")
                    time = f"{year}-{month}-{day}"
                    print(time)
                    for granule_folder in (safe_folder / "GRANULE").glob('*'):  # There is only one GRANULE, but use a loop anyhow
                        # Loop over band groups
                        for band_group, band_group_dict in band_groups.items():
                            bands = band_group_dict["bands"]
                            # Check whether we will create, append or skip (in case the time is already in Zarr)
                            band_group_zarr_group = year_zarr_group.require_group(band_group)
                            print("Checking if Zarr exists")
                            with warnings.catch_warnings(action="ignore"):
                                old_zarr_ds = xr.open_zarr(zarr_store, group=band_group_zarr_group.path)
                                if len(old_zarr_ds.variables) == 0:
                                    print("It does not exist and has to be created")
                                    action = "create"
                                elif time in old_zarr_ds.coords["time"].values:
                                    print("It exists and already contains the time")
                                    continue  # "skip" action
                                else:
                                    print("It exists and doesn't have this time yet")
                                    action = "append"                    
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
                                coords={"time": [time], "band": bands, "y": np.arange(height), "x": np.arange(width)},
                                attrs={"crs": str(crs), "transform": str(transform)}
                            )
                            if action == "create":
                                print("Create")
                                compressor = zarr.codecs.BloscCodec(cname="lz4", clevel=5, shuffle=zarr.codecs.BloscShuffle('bitshuffle'))
                                encoding = {
                                    "data": {
                                        "compressors": compressor,
                                        "chunks": (10, len(bands), band_group_dict["y_chunk_size"], band_group_dict["x_chunk_size"]),  # (Time, band, Y, X) chunk sizes
                                    }
                                }
                                with warnings.catch_warnings(action="ignore"):
                                    ds.to_zarr(zarr_store, mode="w", group=band_group_zarr_group.path, zarr_format=3, encoding=encoding)
                            else:
                                print("Append")
                                with warnings.catch_warnings(action="ignore"):
                                    ds.to_zarr(zarr_store, group=band_group_zarr_group.path, append_dim="time", zarr_format=3)

if __name__ == "__main__":
    convert()