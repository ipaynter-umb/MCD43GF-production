import t_spinup
import t_misc
import t_laads
import datetime
import logging
from os import environ, mkdir, symlink
from os.path import exists
from pathlib import Path
from numpy import arange


# Takes a LAADSDataSet object and a destination directory
def create_symbolic_links(dataset, dest_dir):
    # Get the sorted list of years from the dataset
    years = sorted(dataset.by_year_doy.keys(), key=int)
    # We assume that the first and last years in the dataset bookend the years to be processed.
    # Construct start date
    start_date = datetime.date(year=int(years[0]),
                               month=6,
                               day=20)
    # Construct end date
    end_date = datetime.date(year=int(years[-1]),
                             month=1,
                             day=1) + datetime.timedelta(days=192)
    # Check destination is a path
    if not isinstance(dest_dir, Path):
        # Try converting to a path
        try:
            dest_dir = Path(dest_dir)
        # If it is not able to form a Path object
        except TypeError:
            # Log the error
            logging.error(f'{dest_dir} is not a valid directory path.')
            # Exit
            return None
    # If the destination does not exist
    if not exists(dest_dir):
        # Make it
        mkdir(dest_dir)
    # Target directories for symbolic links
    target_bands = []
    # If the data product name is MCD43D31 or MCD43D40 (which are used by every band)
    if dataset.product == "MCD43D31" or dataset.product == "MCD43D40":
        # The target bands will be all bands
        target_bands = ['01', '02', '03', '04', '05', '06', '07']
    else:
        # Get the band directory name (2-digit zero-padded band)
        target_bands.append(t_misc.zero_pad_number(round((int(dataset.product[-2:]) + 1) / 3), digits=2))
    # For each "middle" year between the bookend years
    for year in list(range(start_date.year + 1, end_date.year)):
        # Year directory
        year_dir = Path(dest_dir, str(year))
        # If there is not a directory yet
        if not exists(year_dir):
            # Make it
            mkdir(year_dir)
        # For each target band
        for band in target_bands:
            # Band dir
            band_dir = Path(year_dir, band)
            # If there is not a directory yet
            if not exists(band_dir):
                # Make it
                mkdir(band_dir)
            # For each year relevant to this middle year
            for subyear in list(range(year - 1, year + 2)):
                # Subyear directory
                subyear_dir = Path(band_dir, str(subyear))
                # If there is not a directory yet
                if not exists(subyear_dir):
                    # Make it
                    mkdir(subyear_dir)
    # For each date_key in the dataset
    for date_key in dataset.by_date.keys():
        # If the date is in the overall window to be used
        if start_date <= date_key <= end_date:
            # For each target band
            for band in target_bands:
                # For each "middle" year between the bookend years
                for year in list(range(start_date.year + 1, end_date.year)):
                    # Construct start date
                    curr_start = datetime.date(year=year - 1,
                                               month=6,
                                               day=20)
                    # Construct end date
                    curr_end = datetime.date(year=year + 1,
                                             month=1,
                                             day=1) + datetime.timedelta(days=192)
                    # If the date is in the current window
                    if curr_start <= date_key <= curr_end:
                        # For each file in the date
                        for file in dataset.by_date[date_key]:
                            # Current file name
                            filename = file.name
                            # Path to the file
                            file_path = Path(environ['inputs_dir'], dataset.name, filename)
                            # Get the link path
                            link_path = Path(dest_dir, str(year), band, str(date_key.year), filename)
                            # Create a symbolic link
                            symlink(file_path, link_path)