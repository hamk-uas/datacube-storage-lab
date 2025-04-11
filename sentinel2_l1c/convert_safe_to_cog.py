import os
import json
import subprocess
import argparse
import shutil
from datetime import datetime
from pathlib import Path
import tempfile
import time

import xmltodict
from osgeo import gdal

from .utils import band_groups

def get_cog_filename(storage_root, tile_id, year, month, day, platform_serial_identifier, item_index, band_group):
    return f"{storage_root}/{tile_id[:2]}/{tile_id[2]}/{tile_id[3:]}/{int(year)}/{int(month)}/S2{platform_serial_identifier}_{tile_id}_{year}{month}{day}_{item_index}_L1C/{band_group}.tif"

def convert(safe_from_folder = os.environ["DSLAB_S2L1C_NETWORK_SAFE_PATH"], cog_to_folder = os.environ["DSLAB_S2L1C_NETWORK_COGS_PATH"]):
    start = time.time()              
    total_num_items = 0
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
                for item_index, safe_folder in enumerate(day_folder.glob('*.SAFE')):
                    total_num_items += 1
                    safe_name = safe_folder.name
                    print("safe_name", safe_name)
                    tile_id = safe_name.split(sep="_")[5][1:]  # For example "35VLH"
                    safe_time = safe_name.split(sep="_")[2]
                    utc_time = f"{safe_time[:4]}-{safe_time[4:6]}-{safe_time[6:8]}T{safe_time[9:11]}:{safe_time[11:13]}:{safe_time[13:]}Z"
                    print(utc_time)  # For example "2024-02-16T09:50:29Z"

                    local_product_metadata_file_name = os.path.join(safe_folder, "MTD_MSIL1C.xml")
                    with open(local_product_metadata_file_name, "r", encoding="utf-8") as file:
                        xml_content = file.read()
                    data_dict = xmltodict.parse(xml_content)
                    if data_dict["n1:Level-1C_User_Product"]["n1:General_Info"]["Product_Info"]["Datatake"]["SENSING_ORBIT_DIRECTION"] == "ASCENDING":
                        continue

                    band_metadata = {}
                    for band_info in data_dict["n1:Level-1C_User_Product"]["n1:General_Info"]["Product_Image_Characteristics"]["Spectral_Information_List"]['Spectral_Information']:
                        band_id = band_info["@physicalBand"]
                        wavelength_min = band_info["Wavelength"]["MIN"]["#text"]
                        wavelength_max = band_info["Wavelength"]["MAX"]["#text"]
                        assert band_info["Wavelength"]["MIN"]["@unit"] == band_info["Wavelength"]["MAX"]["@unit"], f"Mismatching wavelength min max units {band_info["Wavelength"]["MIN"]["@unit"]} {band_info["Wavelength"]["MAX"]["@unit"]} for band {band_id}"
                        wavelength_unit = band_info["Wavelength"]["MIN"]["@unit"]
                        if len(band_id) == 2:
                            band_id = f"{band_id[0]}0{band_id[1]}"
                        band_metadata[band_id] = {
                            "DESCRIPTION": band_id,
                            "WAVELENGTH_UNIT": wavelength_unit,
                            "MIN_WAVELENGTH": wavelength_min,
                            "MAX_WAVELENGTH": wavelength_max,
                        }

                    granule_folder = next((safe_folder / "GRANULE").glob('*'))
                    # Loop over band groups
                    with tempfile.TemporaryDirectory() as temp_dir:
                        for band_group, band_group_dict in band_groups.items():
                            bands = band_group_dict["bands"]
                            band_input_files = [next((granule_folder / "IMG_DATA").glob(f'*{band}.jp2')) for band in bands]
                            temp_merged_fpath = os.path.join(temp_dir, f"merged_{band_group}.tif")                            
                            output_path = get_cog_filename(cog_to_folder, tile_id, year, month, day, safe_name[2], item_index, band_group)
                            output_image_folder = os.path.split(output_path)[0]
                            os.makedirs(output_image_folder, exist_ok=True)
                            actions = [
                                # Using compress=deflate uses 1 min 55 s whereas compress=none took 16 s, so we do not compress temporary files
                                {
                                    "id": "stack_bands",
                                    "description": f"Stacking bands {band_group} to {temp_merged_fpath}", 
                                    "command": ["gdal_merge.py", "-co", "compress=none", "-o", str(temp_merged_fpath), "-separate"] + [str(path) for path in band_input_files]
                                },
                                {
                                    "id": "add_metadata",
                                    "description": f"Adding metadata to {temp_merged_fpath}",
                                },
                                {
                                    "id": "create_cog",
                                    "description": f"Creating COG {output_path}",
                                    "command": ["rio", "cogeo", "create", "--forward-band-tags", str(temp_merged_fpath), str(output_path)]
                                }
                            ]
                            for action in actions:
                                try:
                                    if action["id"] == "add_metadata":
                                        print(action["description"])
                                        dataset = gdal.Open(temp_merged_fpath, gdal.GA_Update)
                                        for band_index, band_id in enumerate(bands):
                                            band = dataset.GetRasterBand(band_index + 1)
                                            band.SetMetadata(band_metadata[band_id])
                                        dataset.FlushCache()
                                        del dataset
                                    else:
                                        print(action["description"])
                                        print(" ".join(action["command"]))
                                        subprocess.run(action["command"], check=True, capture_output=True, text=True)
                                except subprocess.CalledProcessError as e:
                                    raise RuntimeError(str(e), e.stderr)
                        output_metadata_filename = os.path.join(os.path.split(output_path)[0], "MTD_MSIL1C.xml")
                        print(f"Copying metadata file to {output_metadata_filename}")
                        shutil.copyfile(local_product_metadata_file_name, output_metadata_filename)
    duration = time.time() - start
    print("Duration (s):", duration)
    print("Total number of SAFE items:", total_num_items)

if __name__ == "__main__":
    convert()