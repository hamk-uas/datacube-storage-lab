# datacube-storage-lab
Satellite image and weather data intake, format conversion and storage/format load time benchmarks for distributed multimodal machine learning (ML).

Work in progress.

There is a need to evaluate storage systems (mainly those offered by CSC) and storage formats for multi-terabyte spatial data modalities for a ML model operating on multimodal patch timeseries data. Here we provide Python code for intake of these data from external sources, for format conversion, and for benchmarking the alternative solutions.

## Configuration

Configure the [ESA Copernicus Data Space Ecosystem (CDSE) S3 API](https://documentation.dataspace.copernicus.eu/APIs/S3.html) in `~/.aws/configure`:

```
[profile cdse]
endpoint_url = https://eodata.dataspace.copernicus.eu
region = default
```

Configure your CDSE S3 API credentials in `~/.aws/credentials`, filling in your key in place of the x's:

```
[cdse]
aws_access_key_id = xxxxxxxxxxxxxxxxxxxx
aws_secret_access_key = xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

## Sentinel 2 L1C
### Tile list

`sentinel2_l1c/tiles_finland.py` -- Tile ids (strings) of those tiles that intersect with Finland land areas and/or Baltic Sea areas associated with Finland are listed in `tiles_finland` list.

### Intake SAFE

`sentinel2_l1c/intake_cdse_s3.py` -- Download all images from a time range for a given tile using CDSE STAC API and CDSE S3 API.

Example: Download all images from a single tile 35VLH from a single UTC day 2024-02-21:

```
python sentinel2_l1c/intake_cdse_s3.py --tile_id 35VLH --time_start 2024-02-20T00:00:00Z time_end 2024-02-21T00:00:00Z
```

### Intake SAFE (loop)

Querying CDSE STAC API with a large time range brings uncertainties like hitting some API limit and could also lead to pagination of the results which would need to be handled. It is safer to just loop through the days and to make a separate query for each day. Intake will eventually be done on a daily basis anyhow so we have less uncertainties always doing it that way.

`sentinel2_l1c/intake_loop.py` -- Download images for year 2024 for tile 35VLH, with each day queried separately.

Example:

```
python sentinel2_l1c/intake_loop.py
```

### Convert SAFE to COG

### Convert SAFE to Zarr

Zarr scheme:
* One group for each Sentinel 2 L1C location-tile id (for example 35VLH).
    - The original overlap of SAFE tiles is maintained.
    - When the region of interest (patch) intersects with tile overlaps, there will be multiple tiles available to the user to choose from, both having the same orbit number. It would make sense to create a tile
    + Easy to update when new tiles arrive.
    + No need to choose Zarr geographical area (such as Finland) beforehand
* One group for each year
    + No need to fix starting year
    - Last chunk will have nodata values when the number of satellite images is not divisible by chunk size (however it probably compresses away well)
    + This comes after location-tile id group, which is natural in our patch time-series use case.
* One group per resolution (10m, 20m, 60m)
* Time index chunk size: 10
* Band chunk size: the number of bands
* Y chunk size: 512
* X chunk size: 512

Building the Zarr is done the same way as updating it with fresh satellite images, one image at a time. Caching by the Zarr library will be used to reduce transfers (to?)/from the remote Zarr. (see https://github.com/zarr-developers/zarr-python/issues/1500)

At CSC, remember to, on the first time:

```
module load geoconda
python3 -m venv --system-site-packages .venv
source .venv/bin/activate
pip install --upgrade zarr
pip install --upgrade xarray
```

and afterwards just:

```
module load geoconda
source .venv/bin/activate
```

### Time series load time benchmark SAFE/COG/Zarr

## Authors

Olli Niemitalo (Olli.Niemitalo@hamk.fi), Otto Rosenberg

## License

Licensed under the MIT license. We are probably happy to help if you need a different open license.

## Copyright

Copyright 2025 HAMK Häme University of Applied Sciences

## Acknowledgements

The work was supported by the Research Council of Finland funding decision 353076, Digital solutions to foster climate-smart agricultural transition (Digi4CSA). Development and testing were partially done on the CSC – IT Center for Science, Finland supercomputer Puhti.
