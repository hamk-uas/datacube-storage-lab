# datacube-storage-lab
Satellite image and weather data intake, format conversion and storage/format load time benchmarks for distributed multimodal ML.

Work in progress.

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
### Intake SAFE

`python sentinel2_l1c/intake_cdse_s3.py` -- Download all images from a time range for a given tile using CDSE STAC API and CDSE S3 API.

Example: Download all images from a single tile 35VLH from a single UTC day 2024-02-21:

```
python sentinel2_l1c/intake_cdse_s3.py --tile_id 35VLH --time_start 2024-02-20T00:00:00Z time_end 2024-02-21T00:00:00Z
```

### Intake SAFE (loop)

Querying CDSE STAC API with a large time range brings uncertainties like hitting some API limit and could also lead to pagination of the results. It is safer to just loop through the days and to make a separate query each day.

`sentinel2_l1c/intake_loop.py` -- Download images for year 2024 for tile 35VLH, with each day queried separately.

Example:

```
python sentinel2_l1c/intake_loop.py
```

### Convert SAFE to COG

### Convert SAFE to Zarr

### Time series load time benchmark SAFE/COG/Zarr




