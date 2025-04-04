# datacube-storage-lab

Work in progress.

There is a need to evaluate storage systems (for us, mainly those available on CSC – IT Center for Science, Finland supercomputer Puhti) and storage formats for multi-terabyte spatial data modalities for ML models operating on multimodal patch geodata time series. Here we provide Python code for intake of these data from external sources, for format conversion, and for benchmarking alternative storage solutions.

## Prerequisites

We assume Python 3.11 or later.

### Local

For running locally, install dependencies:

TODO: add all dependencies.

```
pip install zarr xarray
```

### CSC Puhti

On CSC Puhti, load the module dependency and create a Python venv with upgraded packages:

```
module load geoconda
python3 -m venv --system-site-packages .venv
source .venv/bin/activate
pip install --upgrade zarr xarray
```

Never mind the error "wrf-python 1.3.4.1 requires basemap" as long as you get a "Successfully installed" last line about xarray and zarr.

On successive jobs you can use the venv again: (in this order, so that the module packages don't mask the venv packages):

```
module load geoconda
source .venv/bin/activate
```

## Configuration

Configure the ESA Copernicus Data Space Ecosystem (CDSE) S3 API endpoint in `~/.aws/config` under a "cdse" profile:

```
[profile cdse]
endpoint_url = https://eodata.dataspace.copernicus.eu
```

For the "cdse" profile, configure your CDSE S3 API credentials in `~/.aws/credentials`, filling in your access key and secret key (see [CDSE S3 API docs](https://documentation.dataspace.copernicus.eu/APIs/S3.html) on creating credentials) in place of the x's:

```
[cdse]
aws_access_key_id = xxxxxxxxxxxxxxxxxxxx
aws_secret_access_key = xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

## Sentinel 2 L1C

The Python scripts in the `sentinel2_l1c` folder handle intake and conversions:

```
ESA CDSE S3 SAFE ---------------> Local SAFE --------------> Local COG
                intake_cdse_s3.py          \ safe_to_cog.py
                by intake_loop.py           \
                                             --------------> Local Zarr
                                             safe_to_zarr.py
```

For benchmarking an S3 storage, first manually copy the data to S3. For CSC Allas:

TODO finalize

```
s3cmd put 
```

For CSC Puhti, in the above diagram, the "local" storage system for intake should be the project scratch (/scratch/project_xxxxxxx). For benchmarking local NVMe storage, the Slurm batch script should first copy the files to NVMe, for example (fill in your user name and project number in place of the x's):

TODO finalize

```shell
#SBATCH --account=project_xxxxxxx
#SBATCH --job-name=dataload
#SBATCH --output=/scratch/project_xxxxxxx/run_%A.txt
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=40
#SBATCH --mem=80G
#SBATCH --partition=small
#SBATCH --gres=nvme:750
#SBATCH --time=3:00:00

cd /scratch/project_xxxxxxx
module load geoconda
source /users/xxxxxxxx/datacube-storage-lab/.venv/bin/activate
rsync -r /scratch/project_xxxxxxx/sentinel-s2-l1c-safe $LOCAL_SCRATCH
rsync -r /scratch/project_xxxxxxx/sentinel-s2-l1c-cogs $LOCAL_SCRATCH
rsync -r /scratch/project_xxxxxxx/sentinel-s2-l1c-zarr $LOCAL_SCRATCH
python3 /users/xxxxxxxx/datacube-storage-lab/sentinel2_l1c/patch_timeseries_benchmark.py . $LOCAL_SCRATCH http://a3s.fi
```

### Tile list

`sentinel2_l1c/tiles_finland.py` -- Tile ids (strings) of those tiles that intersect with Finland land areas and/or Baltic Sea areas associated with Finland are listed in `tiles_finland` list.

### Intake SAFE

`sentinel2_l1c/intake_cdse_s3.py` -- Download all images from a time range for a given tile using CDSE STAC API and CDSE S3 API.

Example: Download all images from a single tile 35VLH from a single UTC day 2024-02-21:

```
python sentinel2_l1c/intake_cdse_s3.py --tile_id 35VLH --time_start 2024-02-21T00:00:00Z time_end 2024-02-22T00:00:00Z
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

### Time series load time benchmark SAFE/COG/Zarr

## Authors

Olli Niemitalo (Olli.Niemitalo@hamk.fi), Otto Rosenberg

## License

Licensed under the MIT license. We are probably happy to help if you need a different open license.

## Copyright

Copyright 2025 HAMK Häme University of Applied Sciences

## Acknowledgements

The work was supported by the Research Council of Finland funding decision 353076, Digital solutions to foster climate-smart agricultural transition (Digi4CSA). Development and testing were partially done on the CSC – IT Center for Science, Finland supercomputer Puhti.
