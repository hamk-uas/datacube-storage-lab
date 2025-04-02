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

`sentinel2_l1c/intake_cdse_s3.py` -- Download all images from a time range for a given tile.

Example: Download all images from a single tile 35WNT from a single UTC day 2024-02-21:

```
python sentinel2_l1c/intake_cdse_s3.py --tile_id 35WNT --time_start 2024-02-20T00:00:00Z time_end 2024-02-21T00:00:00Z
```

### Convert SAFE to COG

### Convert SAFE to Zarr

### Benchmark load times


