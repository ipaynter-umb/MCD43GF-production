import argparse
import t_laads_tools
import datetime
import logging
from os import walk, environ, mkdir, symlink
from os.path import exists
from pathlib import Path
import dotenv
from numpy import arange
from concurrent.futures import ProcessPoolExecutor, as_completed

# Load the environmental variables from .env file
dotenv.load_dotenv()


def get_input_data_for_gapfilled(years, archive_set=6):
    # Set the logging config
    logging.basicConfig(filename=environ['log_files_path'] + f'{datetime.datetime.now():%Y%m%d%H%M%S}.log',
                        filemode='w',
                        format=' %(levelname)s - %(asctime)s - %(message)s',
                        level=logging.INFO)
    # Band list
    band_list = [31, 40]
    # List of bands from 1 - 31
    band_list += list(arange(1, 31, 1))
    # For band in bands
    for band in band_list:
        # Log info
        logging.info(f"Processing band {band}.")
        # Get the input data for the band
        get_input_data_for_band(years, band, archive_set=archive_set)


def get_input_data_for_band(years, band, archive_set=6):
    # Ensure archive set is a string
    archive_set = str(archive_set)
    # Get data library for band
    band_dict = t_laads_tools.EarthDataDict(f"MCD43D{t_laads_tools.zero_pad_number(band, digits=2)}",
                                            archive_set=archive_set)
    # List of targets to be downloaded
    target_list = []
    # List of targets to have their checksums checked
    checksum_check_list = []
    # Ensure years is a list
    years = make_var_list(years)
    # Sort the list
    years = sorted(years, key=int)
    # Construct start date
    start_date = datetime.date(year=int(years[0]) - 1,
                               month=6,
                               day=20)
    # Construct end date
    end_date = datetime.date(year=int(years[-1]) + 1,
                             month=1,
                             day=1) + datetime.timedelta(days=192)
    # Set the current date to the start date
    curr_date = start_date
    # While the current date is <= end date
    while curr_date <= end_date:
        # Get the file object from the date
        file_object = band_dict.get_file_objects_from_date(curr_date)
        # If there is a file object
        if file_object:
            # If more than one file name came back for the date
            if len(file_object) > 1:
                # Log a warning
                logging.warning(f"{len(file_object)} files returned for {curr_date.isoformat()}. Using first file.")
            # Shuck the file object from the list
            file_object = file_object[0]
            # Construct the file path
            file_path = Path(environ['input_files_path'], archive_set, file_object.name)
            # Insert into file object
            file_object.destination = file_path
            # If the file does not exist
            if not exists(file_object.destination):
                # Add EarthDataFileRequest object to list
                target_list.append(file_object)
            # Otherwise (the file does exist)
            else:
                # If there is a checksum
                if file_object.checksum:
                    # Add EarthDataFileRequest object to the checksum check list
                    checksum_check_list.append(file_object)
                # Otherwise (no checksum)
                else:
                    # Log a warning
                    logging.warning(f"Checksum not found for {file_object.name}. Skipping redownload.")
        # Increment the day
        curr_date += datetime.timedelta(days=1)
    # Log info
    logging.info(f"Initial list of {len(target_list)} files to be downloaded.")
    logging.info(f"{len(checksum_check_list)} checksums to check for previously downloaded files.")

    # Note length of target list
    target_list_len = len(target_list)

    # Start a ProcessPoolExecutor
    with ProcessPoolExecutor(max_workers=40) as executor:
        # Submit the tasks from the target list to the worker function
        future_events = {executor.submit(multithread_check_checksum,
                                         target): target for target in checksum_check_list}
        # As each worker is finished
        for event in as_completed(future_events):
            # Get the results dictionary
            checksum_result = event.result()
            # If the checksum check failed
            if checksum_result is False:
                # Log this occurrence
                logging.warning(f"Checksum did not match validation for {future_events[event].name}. Attempting to redownload.")
                # Add the target to the download list
                target_list.append(future_events[event])
        # Add a day to the current date
        curr_date += datetime.timedelta(days=1)
    # Log info
    logging.info(f"{target_list_len - len(target_list)} files need to be redownloaded based on mismatched checksums.")
    logging.info(f"Final list of {len(target_list)} files to be downloaded.")
    # Send the URL list for downloading
    t_laads_tools.multithread_download(target_list, workers=5)


def multithread_check_checksum(target):

    # Log the worker's action
    logging.info(f'Worker checking checksum for {target.name}.')

    # Unpack the object and submit to checksum check, returning the result
    return t_laads_tools.check_checksum(target.destination,
                                        target.checksum)


def create_symbolic_links(years, archive_set):
    # Ensure years is a list
    years = make_var_list(years)
    # Sort the list
    years = sorted(years, key=int)

    # <target year> / band<band> / <sub year> /

    # Link directory path
    links_dir = Path(environ['input_files_path']) / 'links'

    # If there is no directory for the symbolic links
    if not exists(links_dir):
        # Make the directory
        mkdir(links_dir)

    # Links directory path including archive set
    links_dir = links_dir / archive_set

    # If there is no directory for the archive set's symbolic links
    if not exists(links_dir):
        # Make the directory
        mkdir(links_dir)

    # For each year
    for year in years:
        # Construct year path
        year_path = links_dir / str(year)

        # If there is no directory for the year
        if not exists(year_path):
            # Make the directory
            mkdir(year_path)
        # Construct start date
        start_date = datetime.date(year=int(year) - 1,
                                   month=6,
                                   day=20)
        # Construct end date
        end_date = datetime.date(year=int(year) + 1,
                                 month=1,
                                 day=1) + datetime.timedelta(days=192)
        # For each band
        for band in list(arange(1, 8, 1)):
            # Zero pad the band name
            band_name = t_laads_tools.zero_pad_number(band, digits=2)
            # Band path
            band_path = year_path / band_name
            # If there is no directory for the band
            if not exists(band_path):
                # Make the directory
                mkdir(band_path)
            # For each relevant sub-year
            for sub_year in list(arange(int(year) - 1, int(year) + 2, 1)):
                # Sub year path
                sub_path = band_path / str(sub_year)
                # If there is no directory for the band
                if not exists(sub_path):
                    # Make the directory
                    mkdir(sub_path)
            # For each of the relevant MCD43D products (e.g. 01-03 for band 1. All bands use 31 & 40)
            for mcd_product in [t_laads_tools.zero_pad_number(band, digits=2),
                                t_laads_tools.zero_pad_number(band + 1, digits=2),
                                t_laads_tools.zero_pad_number(band + 2, digits=2),
                                "31",
                                "40"]:
                # Get URL library for band
                url_dict = t_laads_tools.EarthDataDict(f"MCD43D{mcd_product}",
                                                       archive_set=archive_set)
                # Set the current date to the start date
                curr_date = start_date
                # While the current date is <= end date
                while curr_date <= end_date:
                    # Get the file name
                    file_name = url_dict.get_urls_from_date(curr_date, file_only=True)
                    # If there is a file name
                    if file_name:
                        # Shuck it from the list
                        file_name = file_name[0]
                        # Construct the file path
                        file_path = Path(environ['input_files_path'], archive_set, file_name)
                        # If the file exists (has been downloaded)
                        if exists(file_path):
                            # Construct path for link
                            link_path = Path(band_path, str(curr_date.year), file_name)
                            # If the path does not exist yet
                            if not exists(link_path):
                                # Create a symbolic link
                                symlink(file_path, Path(band_path, str(curr_date.year), file_name))
                        # Otherwise (file not downloaded?)
                        else:
                            # Log a warning
                            logging.warning(f'Could not create link to {file_name}. File not found.')
                    # Add a day to the current date
                    curr_date += datetime.timedelta(days=1)


def make_var_list(variable):
    # If variable is not a list (i.e. single value)
    if not isinstance(variable, list):
        # Encapsulate in a list
        variable = [variable]
    # Return the list variable
    return variable


if __name__ == '__main__':

    # Make an argument parser instance
    parser = argparse.ArgumentParser()
    # Add an argument for the years
    parser.add_argument('-y',
                        '--years',
                        nargs='+',
                        help='Enter years (space-separated)',
                        required=True)
    # Parse any argument
    args = parser.parse_args()
    # Get input data for years
    get_input_data_for_gapfilled(args.years)
