# datacube-storage-lab

There is a need to evaluate storage systems (for the authors of this repository, mainly those available on CSC – IT Center for Science, Finland supercomputer Puhti) and storage formats for multi-terabyte spatial data modalities for training and serving of machine learning (ML) models operating on multimodal geodata patch time series. In the present repository we provide Python code for intake of such data from external sources, for format conversion, and for benchmarking alternative storage systems and formats.

Data storage benchmark process diagram:

```mermaid
graph LR
    subgraph Storage and format alternatives
        B("Network drive (CSC Puhti project scratch)")
        C("S3 (CSC Allas)")
        D("Temp storage (CSC Puhti node NVMe)")
    end
    A(Primary source)--Intake-->P("Network drive (CSC Puhti project scratch)")
    P--Format conversion-->B
    B--Manual copy-->C("S3 (CSC Allas)")
    B--Scripted copy-->D("Temp storage (CSC Puhti compute node NVMe)")
    B-->E(Random patch data load benchmark)
    C-->E
    D-->E
```

The storage systems that are compared are 1) a network drive (project scratch on CSC Puhti), 2) an S3 object storage (CSC Allas) that mirrors the network drive, and 3) a temp storage (CSC Puhti compute node's local NVMe) that is populated with data from the network drive. Different storage formats are also compared.

The use case that the benchmarking emulates is loading of randomly-located patch time series data for machine learning training. In the actual use case, each compute node may not load data from all satellite image tiles over Finland but from a single tile, or perhaps two tiles. Therefore, the temp storage need not be as large as the full data. Eventually, in machine learning training and serving, intake should store the data directly in the S3 storage rather than the network drive, in the format that is found to be the best in the current benchmarking.

## Prerequisites and configuration

We assume Python 3.11 or later.

### Local

For running locally in Ubuntu Linux, install dependencies:

```
sudo add-apt-repository ppa:ubuntugis/ppa
sudo apt update
sudo apt-get install python3-pip
sudo apt-get install gdal-bin libgdal-dev
sudo apt-get install s3cmd
````

and pip packages (specifying the GDAL version you got from the above, for example `gdal==3.8.4`, if needed to resolve unmet dependencies):

```
pip install numpy zarr xarray pystac_client tenacity dotenv gdal rasterio python-openstackclient xmltodict rio-cogeo dask rioxarray s3fs==2025.3.0 boto3==1.35.36 aiobotocore==2.15.2 botocore==1.35.36
```

### CSC Puhti

For running on CSC Puhti, clone the repository for example to `~/datacube-storage-lab` and work from there. Ensure that the Allas storage service is available to your project (if not, apply). You can work from command line by starting an interactive job from the Puhti login node and waiting in the queue:
```shell
srun --account=project_<PROJECT_NUMBER> --job-name=dslab --ntasks=1 --cpus-per-task=4 --mem=12G --partition=small --time=7:00:00 --pty
```

Choose your CSC project when prompted. Then continue to load the module dependencies and create a Python venv with a few upgraded packages:
```
module load allas
module load geoconda
python3 -m venv --system-site-packages .venv
source .venv/bin/activate
pip install --upgrade zarr xarray dotenv python-openstackclient xmltodict rio-cogeo
```

There may be an error `ERROR: pip's dependency resolver does not currently take into account all the packages that are installed. This behaviour is the source of the following dependency conflicts.
wrf-python 1.3.4.1 requires basemap, which is not installed.` but that's OK as long as you get a `Successfully installed` last line about the upgraded packages.

On sucessive jobs, the Allas mode will persist and you can also just use the same venv again (after module loads so that module packages don't mask venv packages):
```
module load allas
module load geoconda
source .venv/bin/activate
```

### CSC Allas S3

To use Allas object storage from outside CSC Puhti, follow CSC's instructions on [*Configuring S3 connection on local computer*](https://docs.csc.fi/data/Allas/using_allas/s3_client/#getting-started-with-s3cmd). Warning: using `allas_conf` will overwrite any existing `~/.s3cfg` and `~/.aws/credentials`. For this reason it is better to configure Allas first and then configure other S3 credentials.

Configure Allas, specifying a persistent S3 mode rather than Swift mode:
```
allas-conf --mode S3
```

Move the Allas config from `~/.s3cfg` to `~/.s3allas` for clarity:
```
mv ~/.s3cfg ~/.s3allas
```

If you want to, you can rename `s3allas` to something else, in all occurrences in these instructions, including the value of the `DSLAB_S2L1C_S3_PROFILE` environment variable in the next subsection.

Edit `~/.aws/credentials` and change the heading `[default]` to `[s3allas]`.

Edit `~/.aws/config` and add a profile for Allas:

```
[profile s3allas]
endpoint_url = https://a3s.fi
```

### Copernicus Data Space Ecosystem (CDSE) S3 API credentials

To use ESA Copernicus Data Space Ecosystem (CDSE) S3 API as a primary source (as per the workflow documented here), configure its endpoint in `~/.aws/config` under a `cdse` profile. Edit the file and add:

```
[profile cdse]
endpoint_url = https://eodata.dataspace.copernicus.eu
```

For the `cdse` profile, configure your CDSE S3 API credentials by editing `~/.aws/credentials` and by adding the following, filling in your access key and secret key (see [CDSE S3 API docs](https://documentation.dataspace.copernicus.eu/APIs/S3.html) on creating credentials) in place of the placeholders `<CDSE_ACCESS_KEY>` and `<CDSE_SECRET_KEY>`:

```
[cdse]
aws_access_key_id = <CDSE_ACCESS_KEY>
aws_secret_access_key = <CDSE_SECRET_KEY>
```

### Folder and S3 configuration

In the local clone of the present repository, create a file `.env` and configure in it environment variables specifying an S3 profile, data folders/buckets, and a result folder where timestamped result json files will be created by the benchmark. Use the following template tailored for CSC Puhti nodes with NVMe temporary storage (filling in the placefolder `<PROJECT_NUMBER>` for your project number):

```
DSLAB_S2L1C_NETWORK_SAFE_PATH=/scratch/project_<PROJECT_NUMBER>/sentinel2_l1c_safe
DSLAB_S2L1C_NETWORK_COG_PATH=/scratch/project_<PROJECT_NUMBER>/sentinel2_l1c_cog
DSLAB_S2L1C_NETWORK_ZARR_PATH=/scratch/project_<PROJECT_NUMBER>/sentinel2_l1c_zarr
DSLAB_S2L1C_TEMP_SAFE_PATH="${LOCAL_SCRATCH}/sentinel2_l1c_safe"
DSLAB_S2L1C_TEMP_COG_PATH="${LOCAL_SCRATCH}/sentinel2_l1c_cog"
DSLAB_S2L1C_TEMP_ZARR_PATH="${LOCAL_SCRATCH}/sentinel2_l1c_zarr"
DSLAB_S2L1C_S3_PROFILE=s3allas
DSLAB_S2L1C_S3_SAFE_BUCKET=sentinel2_l1c_safe
DSLAB_S2L1C_S3_COG_BUCKET=sentinel2_l1c_cog
DSLAB_S2L1C_S3_ZARR_BUCKET=sentinel2_l1c_zarr
DSLAB_LOG_FOLDER=/scratch/project_<PROJECT_NUMBER>/dslab_logs
```

If you don't use CSC services, change the folders and edit the value of `DSLAB_S2L1C_S3_PROFILE` so that an s3cmd configuration is found at `~/.<DSLAB_S2L1C_S3_PROFILE>` and a configuration and credentials to use with Boto3 are found in `~/.aws/config` under a heading `[profile <DSLAB_S2L1C_S3_PROFILE>]` and in `~/.aws/credentials` under a heading `[<DSLAB_S2L1C_S3_PROFILE>]` with the value of `DSLAB_S2L1C_S3_PROFILE` filled in place of the placeholder `<DSLAB_S2L1C_S3_PROFILE>`. See the above section *Copernicus Data Space Ecosystem (CDSE) S3 API credentials* for an example.

Verify that the following command lists these environment variables (if not, try opening a new terminal):

```shell
env |grep DSLAB_
```

## Running individual modules

The Python modules in this repository typically have a `__main__` function and can therefore be launched from command line. In order to make the `.env` in the repo root findable by a module, the command line should be run from the repo root. For example, to run the module `sentinel2_l1c.intake_cdse_s3_year` which has source code in `sentinel2_l1c/intake_cdse_s3_year.py`:

```
python3 -m sentinel2_l1c.intake_cdse_s3_year
```

Typically you'd follow the workflow as given below. The documentation for each module can also be found after the workflow.

## Sentinel 2 L1C

For Sentinel 2 Level-1C products, we use the free ESA Copernicus Data Space Ecosystem (CDSE) APIS: STAC for tile-based searches and the S3 as the primary source of the data. We do not benchmark the CDSE S3 API because download quota limitations would prevent its use in the intended machine learning use case.

The Python scripts in the `sentinel2_l1c` folder handle intake and conversions. The intake and copying/format conversion to 1) the network drive, 2) a compute node's temp (typically fast NVMe storage on a compute node), and 3) S3 (CSC Allas) and benchmarking is done as follows:

```mermaid
graph LR;
    A(ESA CDSE S3 SAFE)--<code>intake_cdse_s3_year</code>-->B(Network drive SAFE);
    B-->K(Random patch data load benchmark)
    C-->K
    D-->K
    E-->K
    F-->K
    G-->K
    H-->K
    I-->K
    J-->K
    B--<code>Scripted copy</code>--->J(Temp SAFE);
    B--<code>Manual copy</code>--->I(S3 SAFE);    
    B--<code>safe_to_cog</code>-->C(Network drive COG);
    B--<code>safe_to_zarr</code>-->D(Network drive Zarr);
    C--<code>Scripted copy</code>-->E(Temp COG);
    D--<code>Scripted copy</code>-->F(Temp Zarr);
    C--<code>Manual copy</code>-->G(S3 COG);
    D--<code>Manual copy</code>-->H(S3 Zarr);    
    subgraph Storage and format alternatives
        B
        C
        D
        E
        F
        G
        H
        I
        J
    end
```

The standard workflow consists of of the following steps:
1. Sentinel 2 L1C SAFE intake: (slow!)
    ```
    python3 -m sentinel2_l1c.intake_cdse_s3_year   
    ```
2. Convert SAFE to COG and Zarr: (slow!)
    ```
    python3 -m sentinel2_l1c.convert_safe_to_cog
    python3 -m sentinel2_l1c.convert_safe_to_zarr
    ```
3. Manually create S3 buckets for the data in different formats:
    ```
    s3cmd -c ~/.$DSLAB_S2L1C_S3_PROFILE mb s3://$DSLAB_S2L1C_S3_SAFE_BUCKET
    s3cmd -c ~/.$DSLAB_S2L1C_S3_PROFILE mb s3://$DSLAB_S2L1C_S3_COG_BUCKET
    s3cmd -c ~/.$DSLAB_S2L1C_S3_PROFILE mb s3://$DSLAB_S2L1C_S3_ZARR_BUCKET
    ```
4. Manually copy the data to the S3 buckets, making them public: (slow, a few hours)
    ```
    s3cmd -c ~/.$DSLAB_S2L1C_S3_PROFILE put -P -r $DSLAB_S2L1C_NETWORK_SAFE_PATH/ s3://$DSLAB_S2L1C_S3_SAFE_BUCKET/
    s3cmd -c ~/.$DSLAB_S2L1C_S3_PROFILE put -P -r $DSLAB_S2L1C_NETWORK_COG_PATH/ s3://$DSLAB_S2L1C_S3_COG_BUCKET/
    s3cmd -c ~/.$DSLAB_S2L1C_S3_PROFILE put -P -r $DSLAB_S2L1C_NETWORK_ZARR_PATH/ s3://$DSLAB_S2L1C_S3_ZARR_BUCKET/
    s3cmd -c ~/.$DSLAB_S2L1C_S3_PROFILE setpolicy public-read-policy.json s3://$DSLAB_S2L1C_S3_SAFE_BUCKET
    s3cmd -c ~/.$DSLAB_S2L1C_S3_PROFILE setpolicy public-read-policy.json s3://$DSLAB_S2L1C_S3_COG_BUCKET
    s3cmd -c ~/.$DSLAB_S2L1C_S3_PROFILE setpolicy public-read-policy.json s3://$DSLAB_S2L1C_S3_ZARR_BUCKET
    ```
5. Prepare temp storage for benchmarking by copying data to it from network storage: (a bit slow, around 10 minutes)
    ```
    rsync -r $DSLAB_S2L1C_NETWORK_SAFE_PATH/ $DSLAB_S2L1C_TEMP_SAFE_PATH/
    rsync -r $DSLAB_S2L1C_NETWORK_COG_PATH/ $DSLAB_S2L1C_TEMP_COG_PATH/
    rsync -r $DSLAB_S2L1C_NETWORK_ZARR_PATH/ $DSLAB_S2L1C_TEMP_ZARR_PATH/
    ```
6. Benchmark
    ```
    python3 -m sentinel2_l1c.benchmark_patch_load
    ```

On CSC Puhti, for benchmarking a compute node's local NVMe storage, the Slurm batch script below contains steps 5 (copy files from network storage to NVMe storage) and 6 (benchmark) above. Fill in your CSC username and project number in place of the placeholders `<USERNAME>` and `<PROJECT_NUMBER>`.

```shell
#SBATCH --account=project_<PROJECT_NUMBER>
#SBATCH --job-name=dsbench
#SBATCH --output=/scratch/project_<PROJECT_NUMBER>/dataload_%A.txt
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=40
#SBATCH --mem=80G
#SBATCH --partition=small
#SBATCH --gres=nvme:750
#SBATCH --time=3:00:00

cd /users/<USERNAME>/datacube-storage-lab
module load geoconda
source ./.venv/bin/activate
rsync -r $DSLAB_S2L1C_NETWORK_SAFE_PATH/ $DSLAB_S2L1C_TEMP_SAFE_PATH/
rsync -r $DSLAB_S2L1C_NETWORK_COG_PATH/ $DSLAB_S2L1C_TEMP_COG_PATH/
rsync -r $DSLAB_S2L1C_NETWORK_ZARR_PATH/ $DSLAB_S2L1C_TEMP_ZARR_PATH/
python3 -m sentinel2_l1c.benchmark_patch_load
```

The batch script should be stored in a file named `job.sh` or similar, and [submitted](https://docs.csc.fi/computing/running/submitting-jobs/) in CSC Puhti by:

```shell
sbatch job.sh
```

As the benchmarking is a one-time thing you could also start an equivalent interactive job and enter the commands manually, to ensure they work:

```shell
srun --account=project_<PROJECT_NUMBER> --job-name=dataload --ntasks=1 --cpus-per-task=40 --mem=80G --partition=small --gres=nvme:750 --time=3:00:00
```

### Intake SAFE module

To intake Sentinel 2 L1C images for a lengthy time range, do not run `sentinel2_l1c.intake_cdse_s3` directly but instead run `sentinel2_l1c.intake_cdse_s3_year` for yearly intake, described in the next section.

`python3 -m sentinel2_l1c.intake_cdse_s3` — Download all Sentinel2 L1C SAFE-format images within a time range for a given tile using the CDSE STAC API and CDSE S3 API. 

Command line arguments:
* `--time_start <STRING>` — Start time in UTC format (`YYYY-MM-DDTHH:MM:SSZ`), default:  `2024-02-21T00:00:00Z`
* `--time_end <STRING>` — End time (not included) in UTC format (`YYYY-MM-DDTHH:MM:SSZ`), default: `2024-02-22T00:00:00Z`
* `--tile_id <STRING>` — Tile identifier, default: `35VLH`

Example: Download all images from a single tile 35VLH from a single UTC day 2024-02-21:

```
python3 -m sentinel2_l1c.intake_cdse_s3 --tile_id 35VLH --time_start 2024-02-21T00:00:00Z --time_end 2024-02-22T00:00:00Z
```

### Intake SAFE year module

Querying the CDSE STAC API with a large time range brings uncertainties like hitting some API limit and could also lead to pagination of the results which would need to be handled. It is safer to just loop through the days and to make a separate query for each day.

`python3 -m sentinel2_l1c.intake_cdse_s3_year` — Download images for a full UTC year for the given tile, by looping over UTC days.

Command line arguments:
* `--year_start <INT>` — Start year, default: `2024`
* `--year_end <INT>` — End year (not included), default: `2025`
* `--tile_id <STRING>` — Tile identifier, default: `35VLH`

Example: Download images for UTC year 2024 for tile 35VLH:

```
python3 -m sentinel2_l1c.intake_cdse_s3_year --year_start 2024 --year_end 2025 --tile_id 35VLH
```

### Convert SAFE to COG module

`python3 -m sentinel2_l1c.convert_safe_to_cog` — Convert all collected Sentinel 2 L1C SAFE format images in `$DSLAB_S2L1C_NETWORK_SAFE_PATH` to COGs in `$DSLAB_S2L1C_NETWORK_COG_PATH`. There are no command line arguments. The source SAFE files will not be removed or altered.

The conversion to COG is done by stacking all images at each 10m, 20m, and 60m resoluton into a temporary uncompressed GeoTIFF, by adding metadata, and by creating for each resolution a COG using [rio-cogeo](https://cogeotiff.github.io/rio-cogeo/) with default arguments. This results in using Deflate compression and chunk sizes 512x512 at each resolution and also creates overviews at a few fractional resolutions.

### Convert SAFE to Zarr module

`python3 -m sentinel2_l1c.convert_safe_to_zarr` — Convert all collected Sentinel 2 L1C SAFE format images in `$DSLAB_S2L1C_NETWORK_SAFE_PATH` to Zarr in `$DSLAB_S2L1C_NETWORK_ZARR_PATH`. There are no command line arguments. The source SAFE files will not be removed or altered.

This should not be considered as a reference implementation of SAFE to Zarr conversion because it does not include metadata from MTD_MSIL1C.xml (such as millisecond precision datetime) or other SAFE format metadata files, does not include nodata masks, stores CRS information in a hacky string format, and does not have an optimal bucket–group split for CSC Allas which has limitations on the number of buckets and the number of objects in a bucket.

The conversion is not Dask-parallelized at SAFE level but Zarr may have its own internal parallelization.

Zarr is a cloud-native format for rectangular multidimensional arrays. Arrays reside inside nested "groups" in a Zarr "store". We will have a Zarr group hierarchy (in root to branch order): tile, year, band group.

Zarr v3 consists of metadata JSON files (or objects in object storage) and compressed chunks of data in subfolders. A chunk size must be chosen for each dimension. The dimensions of our arrays are: time, band, y, x. We will use different chunk sizes for band groups at different resolutions (with "max" denoting to use the number of bands as the chunk size):

Chunk sizes for time, band, y, x:
* 10 m resolution: 1, max, 512, 512 
* 20 m resolution: 1, max, 256, 256
* 60 m resolution: 1, max, 128, 128

### Benchmark load times module

`python3 -m sentinel2_l1c.benchmark_patch_load` — Benchmark loading of patch time series data for random 5100m x 5100m patches (divisible by 10m, 20m, and 60m) within a single Sentinel 2 L1C tile, over a single year. Dask parallelization is used in loading. The year is determined automatically from one of the SAFE items. See the earlier section *Folder and S3 configuration* on configuring the storage paths. At a given repeat number, the benchmark will always use the same random number generator seed and should produce identical patches and identical shuffled storage and format orders for each run of the benchmark, unless the number of storages or formats is changed. In S3 SAFE and S3 Zarr benchmarks, network storage files are used to determine the corresponding object paths in S3. This emulates a catalog stored in the network storage.

Command line options:
* `--storages <SPACE-SEPARATED STRINGS>` — Storages to benchmark, default: `network temp s3`
* `--formats <SPACE-SEPARATED STRINGS>` — Formats to benchmark, default: `safe cog zarr`
* `--num_repeats <INTEGER>` — Number of repeat (2 or more), default: `10`
* `--year <INTEGER>` — Year for which to load data, default: autodetected from SAFE
* `--tile <STRING>` — Tile id for which to load data, default: autodetected from SAFE
* `--x1 <INTEGER>` — Horizontal position of top left corner of tile in tile UTM zone CRS, default: autodetected from SAFE
* `--y1 <INTEGER>` — Vertical position of top left corner of tile in tile UTM zone CRS, default: autodetected from SAFE
* `--x2 <INTEGER>` — Horizontal position of bottom right corner of tile in tile UTM zone CRS, default: autodetected from SAFE
* `--y2 <INTEGER>` — Vertical position of bottom right corner of tile in tile UTM zone CRS, default: autodetected from SAFE

In preparation for benchmarking, intake should have been done just for a single tile and a single year and intake, format conversions, and copying to different storages must have completed. Otherwise different storages and formats may have slightly different but this can be verified from results.

The results will be written in `$DSLAB_LOG_FOLDER/sentinel2_l1c_YYYY-MM-DD_HH-mm-SS.json` with the benchmark start datetime embedded in the file name. Example results with only the storage `network` and the format `cog` benchmarked follows. The durations are in seconds. Summary statistics are included. An initial warmup run (not counted in `num_repeats`) is done that is not reported in the results and does not affect the statistics. The `band_group_shapes` property can be compared between different storages and formats to ensure they loaded the same amount of data.

```json
{
    "tile": "35VLH",
    "year": 2024,
    "results": {
        "network": {
            "cog": {
                "durations": [
                    38.28519129753113,
                    38.561540842056274,
                    48.16329765319824,
                    42.332008600234985,
                    38.86043119430542,
                    38.90768003463745,
                    34.59686803817749,
                    28.583977699279785,
                    36.84263896942139,
                    37.812105655670166
                ],
                "band_group_shapes": {
                    "B01_B09_B10": [
                        192,
                        3,
                        85,
                        85
                    ],
                    "B02_B03_B04_B08": [
                        192,
                        4,
                        510,
                        510
                    ],
                    "B05_B06_B07_B8A_B11_B12": [
                        192,
                        6,
                        255,
                        255
                    ]
                },
                "total_duration": 382.94573998451233,
                "mean_durations": 38.294573998451234,
                "std_durations": 4.7409876432052505,
                "stderr_durations": 0.47409876432052506
            }
        }
    }
}
```

## Authors

Olli Niemitalo (Olli.Niemitalo@hamk.fi), Otto Rosenberg

## License

Licensed under the MIT license. We are probably happy to help if you need a different open license.

## Copyright

Copyright 2025 HAMK Häme University of Applied Sciences

## Acknowledgements

The work was supported by the Research Council of Finland funding decision 353076, Digital solutions to foster climate-smart agricultural transition (Digi4CSA). Development and testing were partially done on the CSC – IT Center for Science, Finland supercomputer Puhti.
