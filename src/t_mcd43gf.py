import argparse
import t_laads_tools
import datetime
import logging
from os import walk, environ, mkdir, symlink
from os.path import exists
from pathlib import Path
import dotenv
from numpy import arange

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
        # Get the input data for the band
        get_input_data_for_band(years, band, archive_set=archive_set)


def get_input_data_for_band(years, band, archive_set=6):
    # Ensure archive set is a string
    archive_set = str(archive_set)
    # Get data library for band
    band_dict = t_laads_tools.EarthDataDict(f"MCD43D{t_laads_tools.zero_pad_number(band, digits=2)}",
                                            archive_set=archive_set)
    # List of URLs to be downloaded
    target_list = []
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
        # Get the file name
        file_name = band_dict.get_urls_from_date(curr_date,
                                                 file_only=True)
        # If there is a file name
        if file_name:
            # If more than one file name came back for the date
            if len(file_name) > 1:
                # Log a warning
                logging.warning(f"{len(file_name)} file names returned for {curr_date.isoformat()}. Using first file name.")
            file_name = file_name[0]
            # Construct the file path
            file_path = Path(environ['input_files_path'] + archive_set + file_name)
            # If the file does not exist
            if not exists(file_path):
                # Get the URL (will return None is the URL is not available)
                url = band_dict.get_urls_from_date(curr_date,
                                                   file_only=False)
                # If there was a URL
                if url:
                    # Get the checksum
                    checksum = band_dict.by_name.get(file_name)['checksum']
                    # If more than one URL came back for the date
                    if len(url) > 1:
                        # Log a warning
                        logging.warning(f"{len(url)} URLs returned for {curr_date.isoformat()}. Using first URL.")
                    url = url[0]
                    # Add EarthDataFileRequest object to list
                    target_list.append(t_laads_tools.EarthDataFileRequest(file_name,
                                                                          url,
                                                                          file_path,
                                                                          checksum=checksum))
            # Otherwise (file does exist)
            else:
                # Try and get the checksum
                checksum = band_dict.by_name.get(file_name)['checksum']
                # If there is a checksum
                if checksum:
                    # If the checksum is wrong
                    if not t_laads_tools.check_checksum(file_path, checksum):
                        # Log this occurrence
                        logging.warning(f"Checksum did not match validation for {file_name}. Attempting to redownload.")
                        # Get the URL (will return None is the URL is not available)
                        url = band_dict.get_urls_from_date(curr_date,
                                                           file_only=False)
                        # If there was a URL
                        if url:
                            # Get the checksum
                            checksum = band_dict.by_name.get(file_name)['checksum']
                            # If more than one URL came back for the date
                            if len(url) > 1:
                                # Log a warning
                                logging.warning(f"{len(url)} URLs returned for {curr_date.isoformat()}. Using first URL only.")
                            url = url[0]
                            # Add EarthDataFileRequest object to list to redownload
                            target_list.append(t_laads_tools.EarthDataFileRequest(file_name,
                                                                                  url,
                                                                                  file_path,
                                                                                  checksum=checksum))
                # Otherwise (no checksum)
                else:
                    # Log a warning
                    logging.warning(f"Checksum not found for {file_name}. Skipping download.")
        # Add a day to the current date
        curr_date += datetime.timedelta(days=1)
    # Send the URL list for downloading
    t_laads_tools.multithread_download(target_list, workers=5)


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
                        # Construct the file path
                        file_path = Path(environ['input_files_path'] + file_name)
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