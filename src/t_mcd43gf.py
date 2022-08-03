import argparse
import t_laads_tools
import datetime
import logging
from os import walk, environ
from os.path import exists
from pathlib import Path
import dotenv
from numpy import arange

# Load the environmental variables from .env file
dotenv.load_dotenv()


def get_input_data_for_gapfilled(years):
    # Set the logging config
    logging.basicConfig(filename=environ['log_files_path'] + f'{datetime.datetime.now():%Y%m%d%H%M%S}.log',
                        filemode='w',
                        format=' %(levelname)s - %(asctime)s - %(message)s',
                        level=logging.NOTSET)
    # List of bands from 1 - 31
    band_list = list(arange(1, 32, 1))
    # Add band 40
    band_list.append(40)
    # For band in bands
    for band in band_list:
        # Get the input data for the band
        get_input_data_for_band(years, band)


def get_input_data_for_band(years, band):
    # Get URL library for band
    url_dict = t_laads_tools.LaadsUrlsDict(f"MCD43D{t_laads_tools.zero_pad_number(band, digits=2)}",
                                           archive_set=6)
    # List of URLs to be downloaded
    url_list = []
    # If years is not a list (i.e. single year)
    if not isinstance(years, list):
        # Encapsulate in a list
        years = [years]
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
        file_name = url_dict.get_url_from_date('global', curr_date, file_only=True)
        # If there is a file name
        if file_name:
            # Construct the file path
            file_path = Path(environ['input_files_path'] + file_name)
            # If the file does not exist
            if not exists(file_path):
                # Get the URL (will return None is the URL is not available)
                url = url_dict.get_url_from_date('global', curr_date, file_only=False)
                # If there was a URL
                if url:
                    # Add to list
                    url_list.append(url)
        # Add a day to the current date
        curr_date += datetime.timedelta(days=1)
    # Send the URL list for downloading
    t_laads_tools.multithread_download(url_list, workers=5)


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