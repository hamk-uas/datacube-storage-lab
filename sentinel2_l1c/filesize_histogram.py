import matplotlib.pyplot as plt
import numpy as np
from itertools import cycle
import subprocess
from .utils import band_groups

def histogram():
    # Get the default color cycle from matplotlib
    colors = plt.rcParams['axes.prop_cycle'].by_key()['color']
    color_iterator = cycle(colors)

    # Calculate the overall range of file sizes to determine common bins
    all_file_sizes = []
    resolution_to_band_group = dict(sorted({f"{band_groups[band_group]['resolution']}m": band_group for band_group in band_groups}.items()))
    file_sizes = {resolution: [] for resolution in resolution_to_band_group}

    for resolution in resolution_to_band_group:
        sys_call = f"find $DSLAB_S2L1C_NETWORK_ZARR_PATH -type f -exec du --apparent-size --block-size=1 {{}} + |grep {resolution_to_band_group[resolution]}"
        with subprocess.Popen(sys_call, stdout=subprocess.PIPE, shell=True, text=True).stdout as file:
            for line in file:
                size = int(line.split()[0])
                size_MiB = size / (1024 * 1024)  # Convert to MiB
                all_file_sizes.append(size_MiB)
                file_sizes[resolution].append(size_MiB)

    # Calculate bin edges based on overall range
    _, bins = np.histogram(all_file_sizes, bins=50)

    fig, ax = plt.subplots()  # Create a single figure and axes

    for i, resolution in enumerate(resolution_to_band_group.keys()):
        file_sizes[resolution]

        # Calculate and label the mean
        mean_size = np.mean(file_sizes[resolution])
        
        # Use the color from the 'colors' list for both the histogram and the mean line
        color = next(color_iterator)
        ax.hist(file_sizes[resolution], bins=bins, alpha=0.5, label=f'{resolution} (mean={mean_size:.2f} MiB)', color=color)  
        ax.axvline(mean_size, color=color, linestyle='dashed', linewidth=1)  

    ax.set_xlabel('File size (MiB)')
    ax.set_ylabel('Frequency')
    ax.set_title('Histogram of file sizes')
    ax.legend()  # Show legend with labels
    # Save figure to file:
    fig.savefig('histogram_sentinel2_l1c.png', bbox_inches='tight')

if __name__ == "__main__":
    histogram()