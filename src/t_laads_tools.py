import requests
import json
from time import sleep
import h5py
import io
import datetime
import dotenv
from os import environ, walk
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from numpy import around
from time import time
import logging

# Load the .env file
dotenv.load_dotenv()

# Class for URL dictionary (to load when you need it)
class LaadsUrlsDict:

    __slots__ = ["dictionary", "data_product", "archive_set"]

    def __init__(self, data_product, archive_set="5000"):

        # Instantiate attributes
        self.dictionary = None
        self.data_product = data_product
        self.archive_set = str(archive_set)
        # Latest date
        latest_date = None
        # URl dictionary instantiation
        urls_dict = None
        # Walk the support file directory
        for root, dirs, files in walk(environ["support_files_path"]):
            # For each file name
            for name in files:
                # If the file is one of the URL files
                if f"{archive_set}_{data_product}_laads_urls" in name:
                    # Split the name
                    split_name = name.split('_')
                    # Make a datetime date object from the name
                    file_date = datetime.date(year=int(split_name[-1][4:8]),
                                              month=int(split_name[-1][0:2]),
                                              day=int(split_name[-1][2:4]))
                    # If there is no latest date yet
                    if latest_date is None:
                        # Set the file's date
                        latest_date = file_date
                    # Otherwise, if the file's date is later
                    elif file_date > latest_date:
                        # Set the file's date as latest
                        latest_date = file_date
        # If there was a file (as indicated by the presence of a latest date)
        if latest_date is not None:
            # Print and log update
            print(f"Opening URLs file from {latest_date}")
            logging.info(f"Opening URLs file from {latest_date}")
            # Assemble the path to the file
            latest_file_path = Path(
                environ["support_files_path"] + f'{self.archive_set}_{self.data_product}_laads_urls_' + latest_date.strftime(
                    "%m%d%Y") + ".json")
            # Open the file
            with open(latest_file_path, 'r') as f:
                # Load as dictionary
                urls_dict = json.load(f)
            # Reference dictionary
            self.dictionary = urls_dict
        # Otherwise (no file)
        else:
            # Update the file (this will reference the dictionary)
            get_VIIRS_availability(data_product,
                                   existing_dict=self,
                                   archive_set=self.archive_set)

    # Get a LAADS url from a datetime object
    def get_url_from_date(self, tile, date, file_only=False):
        # <> Generalized to a support database/table that contains keywords like "monthly", "annual"
        # If the data product is VNP46A3
        if self.data_product == "VNP46A3":
            # Set the day to 1
            date = date.replace(day=1)
        # If the data product is VNP46A4
        elif self.data_product == "VNP46A4":
            # Set the day and month to 1
            date = date.replace(day=1)
            date = date.replace(month=1)
        # If the tile is in the dictionary
        if tile in self.dictionary.keys():
            # If the year is in the subdictionary
            if str(date.year) in self.dictionary[tile].keys():
                # Get the day of year
                doy = zero_pad_number((date - datetime.date(year=date.year, month=1, day=1)).days + 1)
                # If the doy is in the subdictionary
                if doy in self.dictionary[tile][str(date.year)].keys():
                    # Get the file name
                    file_name = self.dictionary[tile][str(date.year)][doy]
                    # If only the file name was request
                    if file_only:
                        # Return the filename
                        return file_name
                    # Otherwise (full URL)
                    else:
                        # Assemble the full URL
                        full_url = environ["laads_alldata_url"]
                        full_url += self.archive_set + '/' + self.data_product + '/' + str(date.year) + '/'
                        full_url += doy + '/' + file_name
                        # Return the full url
                        return full_url
        # Return None
        return None

    # Get the urls for a specified date range, or date list
    # <> Min year is based on VIIRS life span
    def get_urls_from_date_range(self,
                                 tile="h00v00",
                                 start_date=datetime.date(year=2011, month=1, day=1),
                                 end_date=datetime.datetime.now()):
        # List for URLs
        urls_list = []
        # Check we have information for the tile
        if tile not in self.dictionary.keys():
            # Print a warning
            print(f"Warning tile {tile} not found in the URL dictionary.")
            logging.warning(f"Warning tile {tile} not found in the URL dictionary.")
            # End
            return urls_list
        # Specify target date
        target_date = start_date
        # While the target date is before end date
        while target_date <= end_date:
            # Get the URL (if any)
            target_url = self.get_url_from_date(tile, target_date)
            # If there was a URL
            if target_url:
                # Append the URL to the list
                urls_list.append(target_url)
            # Add a day to target date
            target_date += datetime.timedelta(days=1)
        # Return the url list
        return urls_list


# Function for our multi-threaded workers to use
def multithread_download_function(target_url):
    # Start time for the file
    ptime = time()
    # Start a LAADS session (the requests session object is not thread-safe so we need one per thread)
    s = connect_to_laads()
    # Get the requested file from the URL
    downloaded = get_VIIRS_file(s, target_url, write_local=True, return_file=False, return_content=False)
    # Print the time taken
    print(f"{target_url.split('/')[-1]} downloaded in {around(time() - ptime, decimals=2)} seconds.")
    logging.info(f"{target_url.split('/')[-1]} downloaded in {around(time() - ptime, decimals=2)} seconds.")


# Function for multi-threaded downloading
def multithread_download(target_urls, workers=3):

    # Mark start time
    stime = time()
    # Guard against single URLs submitted
    if not isinstance(target_urls, list):
        # Encapsulate in list
        target_urls = [target_urls]

    # Start a ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Submit the tasks from the url list to the worker function
        future_events = {executor.submit(multithread_download_function, target_url): target_url for target_url in
                         target_urls}

    # Report on the overall time taken
    print(f"All downloads finished in {around(time() - stime, decimals=2)} seconds.")
    logging.info(f"All downloads finished in {around(time() - stime, decimals=2)} seconds.")


# Function to submit request to LAADS and keep trying until we get a response
def try_try_again(r, s, target_url):

    # Back-off timer
    back_off = 5
    # If we get timed out
    while r.status_code != 200:
        # Print a warning
        print(f'Warning, bad response for {target_url}.')
        logging.warning(f'Warning, bad response for {target_url}.')
        # Wait a hot second
        sleep(back_off)
        # Try again
        r = s.get(target_url)
        # Add to back off timer
        back_off += 1
    # Return the completed request
    return r


# Connect to LAADS and return a session object
def connect_to_laads():
    # Header command utilizing security token
    authToken = {'Authorization': f'Bearer {environ["laads_token"]}'}
    # Create session
    s = requests.session()
    # Update header with authorization
    s.headers.update(authToken)
    # Return the session object
    return s


# Get a VIIRS H5 file from laads and return it in some form
def get_VIIRS_file(session_obj, target_url, write_local=False, return_content=False, return_file=True):
    # Request the H5 file from the provided URL
    r = session_obj.get(target_url)
    # If the request failed
    if r.status_code != 200:
        # Send to repeated submission function
        r = try_try_again(r, session_obj, target_url)
    # Try to convert into an h5 object
    try:
        # If write to disk
        if write_local is True:
            with open(environ["output_files_path"] + target_url.split('/')[-1], 'wb') as f:
                f.write(r.content)
        # If content
        if return_content is True:
            return r.content
        # Convert to h5 file object (checks integrity)
        # <> Replace with checksum
        h5file = h5py.File(io.BytesIO(r.content), 'r')
        # If we are returning the file
        if return_file:
            # Convert the response content to an H5py File object and return
            return h5file
        # If returning nothing else, but successfully reached this point, return True
        return True
    # If it fails (incomplete file)
    except:
        # Print a warning
        print(f'Warning: File {target_url} could not be converted to h5. Possibly incomplete.')
        logging.warning(f'Warning: File {target_url} could not be converted to h5. Possibly incomplete.')
        # Return None
        return None


# Function to return a dictionary of URLs to a VIIRS product on LAADS (will update existing)
def get_VIIRS_availability(data_product,
                           start_fresh=False,
                           check_old_gaps=False,
                           cleanup_old_files=False,
                           existing_dict=None,
                           archive_set="5000"):
    # Guard against non-string archive sets
    archive_set = str(archive_set)
    # If there is no existing URLs dict object
    if existing_dict is None:
        # Instantiate a URLs dict object
        urls_dict = LaadsUrlsDict(data_product, archive_set=archive_set)
    # Otherwise (existing dictionary triggered this availability update)
    else:
        # Use the dictionary
        urls_dict = existing_dict
    # If there is no urls dict yet
    if urls_dict.dictionary is None:
        # Turn it into an empty dictionary
        urls_dict.dictionary = {}
    # Target URL for laads data
    target_url = environ["laads_alldata_url"] + archive_set + '/' + data_product + ".json"
    # Get a laads session
    laads_session = connect_to_laads()
    # Get the years in json format from the target URL
    r = laads_session.get(target_url)
    # If the request failed
    if r.status_code != 200:
        # Send to repeated submission function
        r = try_try_again(r, laads_session, target_url)
    # Load the content of the response
    years = json.loads(r.text)
    # For each year in the data
    for year in years:
        # Get year value
        year_value = year["name"]
        # Construct year URL
        year_url = target_url.replace(".json", f"/{year_value}.json")
        # Get the days (adding the year to the original URL
        r = laads_session.get(year_url)
        # If the request failed
        if r.status_code != 200:
            # Send to repeated submission function
            r = try_try_again(r, laads_session, year_url)
        # Load the data as text
        days = json.loads(r.text)
        # For each day
        for day in days:
            # Retrieve day value
            day_value = day["name"]
            # Construct day URL
            day_url = target_url.replace(".json", f"/{year_value}/{day_value}.json")
            # Get the tiles (adding the day and year to the URL)
            r = laads_session.get(day_url)
            print(f"Processing: Archive set {archive_set}, product {data_product}, for {year_value}, day of year: {day_value}.")
            logging.info(f"Processing: Archive set {archive_set}, product {data_product}, for {year_value}, day of year: {day_value}.")
            # If the request failed
            if r.status_code != 200:
                # Send to repeated submission function
                r = try_try_again(r, laads_session, day_url)
            # Load the data as text
            tiles = json.loads(r.text)
            # For each of the tiles
            for tile in tiles:
                # Pull the file name of the tile
                file_name = tile["name"]
                # Split the name on the periods
                split_name = file_name.split('.')
                # Extract the tile name
                tile_name = split_name[2]
                # Check if this is really a tile name
                if tile_name[0] != 'h':
                    # Substitute in global
                    tile_name = 'global'
                # Store the filename in the urls dictionary
                # If the tile is not in the dict yet
                if tile_name not in urls_dict.dictionary.keys():
                    # Add it as a key with an empty subdict value
                    urls_dict.dictionary[tile_name] = {}
                # If the year is not in the tile subdict as a key
                if year_value not in urls_dict.dictionary[tile_name].keys():
                    # Add it as a key with an empty subdict value
                    urls_dict.dictionary[tile_name][year_value] = {}
                # Add or replace the filename for the DOY, for the year, for the tile
                urls_dict.dictionary[tile_name][year_value][day_value] = file_name
    # Get today's date as a string
    file_date = datetime.datetime.now().strftime("%m%d%Y")
    # Construct save path
    save_path = environ["support_files_path"] + f'{archive_set}_{data_product}_laads_urls_{file_date}.json'
    # Write the dictionary
    with open(save_path, 'w') as of:
        json.dump(urls_dict.dictionary, of, indent=4)
    # Close the session
    laads_session.close()


def zero_pad_number(input_number, digits=3):
    # Make sure the number has been converted to a string
    input_number = str(input_number)
    # While the length of the string is less than the required digits
    while len(input_number) < digits:
        # Prepend a 0 to the string
        input_number = '0' + input_number
    # Return the string
    return input_number


# Get a DOY from a datetime object (specify zero pad digits in zero_pad kwarg)
def get_doy_from_date(date, zero_pad=None):
    # Get the day of year
    doy = (date - datetime.date(year=date.year, month=1, day=1)).days + 1
    # If there is a zero pad requested
    if zero_pad:
        # Zero pad the number
        doy = zero_pad_number(doy, zero_pad)
    # Return the doy
    return doy


# Spidering LAADS URLs for multiple products using multithreading
def multithread_spidering(data_products,
                          archive_set="5000",
                          workers=3):
    # If data products are not a list
    if not isinstance(data_products, list):
        # Encapsulate in a list
        data_products = [data_products]
    # If archive sets are not a list
    #if not isinstance(archive_sets, list):
        # Encapsulate in a list
        #archive_sets = [archive_sets]
    # If the length of the archive sets is 1
    #if len(archive_sets) == 1:
        #
    # Start multithread pool
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_events = {executor.submit(LaadsUrlsDict,
                                         product,
                                         archive_set=archive_set): product for product in data_products}


def main():

    pass


if __name__ == '__main__':

    main()




