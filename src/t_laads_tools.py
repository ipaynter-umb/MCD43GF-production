import os
import requests
import json
from time import sleep
import h5py
import io
import datetime
import dotenv
from os import environ, walk
from os.path import exists
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from numpy import around
from time import time
import logging
import traceback
import lxml.html as lh

# Load the .env file
dotenv.load_dotenv()


# Class for URL dictionary (to load when you need it)
class LaadsUrlsDict:

    __slots__ = ["by_date",
                 "by_id",
                 "by_name",
                 "data_product",
                 "archive_set"]

    def __init__(self, data_product, archive_set="5000"):

        # Instantiate attributes
        self.by_date = {}
        self.by_id = {}
        self.by_name = {}
        self.data_product = data_product
        self.archive_set = str(archive_set)
        # Latest date
        latest_date = self.get_latest_url_file()
        # If there was a file (as indicated by the presence of a latest date)
        while latest_date is None:
            # Get the file details to make a file
            get_product_ids_urls_checksums(self.data_product,
                                           self.archive_set,
                                           workers=3)
            # Latest date
            latest_date = self.get_latest_url_file()
        # Print and log update
        logging.info(f"Opening file details from {latest_date}")
        # Assemble the path to the file
        latest_file_path = Path(
            environ["support_files_path"] + f'{self.archive_set}_{self.data_product}_files_' + latest_date.strftime(
                "%m%d%Y") + ".json")
        # Open the file
        with open(latest_file_path, 'r') as f:
            # Load as dictionary and reference
            input_dict = json.load(f)
        # Sort the dictionary into the various attributes
        self.ingest_dict(input_dict)

    # Ingest a dictionary into different sorting methods
    def ingest_dict(self, input_dict):
        # For each key (File ID) in the dictionary
        for id in input_dict.keys():
            # Construct the subdictionary
            subdict = {'id': id,
                       'name': input_dict[id]['name'],
                       'checksum': input_dict[id]['checksum'],
                       'url': input_dict[id]['url']}
            # Store under the ID
            self.by_id[id] = subdict
            # If there is no entry for the file name
            if input_dict[id]['name'] not in self.by_name.keys():
                # Add it
                self.by_name[input_dict[id]['name']] = subdict
            # Otherwise
            else:
                logging.warning(f"Duplicate entry found for file name {subdict['name']}")
            # Extract the date details from the file name
            split_date = subdict['name'].split('.')[1]
            year = split_date[1:5]
            doy = split_date[5:8]
            # If there is no year entry
            if year not in self.by_date.keys():
                # Add the year
                self.by_date[year] = {}
            # If there is no DOY entry
            if doy not in self.by_date[year].keys():
                # Add the subdictionary
                self.by_date[year][doy] = subdict
            # Otherwise (duplicate DOY in year)
            else:
                # Log a warning
                logging.warning(f"Duplicate entry found for year {year}, DOY {doy}.")

    # Get the latest date of a URL file
    def get_latest_url_file(self):
        # Latest date variable
        latest_date = None
        # Walk the support file directory
        for root, dirs, files in walk(environ["support_files_path"]):
            # For each file name
            for name in files:
                # If the file is one of the URL files
                if f"{self.archive_set}_{self.data_product}_files_" in name:
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
        # Return latest date
        return latest_date

    # Get file url(s) from a datetime object
    def get_urls_from_date(self, date, file_only=False):
        # URL list
        url_list = []
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
        # If the year is in the subdictionary
        if str(date.year) in self.by_date.keys():
            # Get the day of year
            doy = zero_pad_number((date - datetime.date(year=date.year, month=1, day=1)).days + 1)
            # If the doy is in the subdictionary
            if doy in self.by_date[str(date.year)].keys():
                # If only the file name was request
                if file_only:
                    # Append the filename
                    url_list.append(self.by_date[str(date.year)][doy]['name'])
                # Otherwise (full URL)
                else:
                    # Append the full url
                    url_list.append(self.by_date[str(date.year)][doy]['url'])
        # If the list is empty
        if len(url_list) == 0:
            # Return None
            return None
        # Return the list
        return url_list

    # Get the urls for a specified date range, or date list
    # <> Min year is based on VIIRS life span
    def get_urls_from_date_range(self,
                                 tile="h00v00",
                                 start_date=datetime.date(year=2011, month=1, day=1),
                                 end_date=datetime.datetime.now()):
        # List for URLs
        urls_list = []
        # Specify target date
        target_date = start_date
        # While the target date is before end date
        while target_date <= end_date:
            # Get the URL (if any)
            target_url = self.get_urls_from_date(target_date)
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
    downloaded = get_product_file(s, target_url, write_local=True, return_file=False, return_content=False)
    # Print the time taken
    print(f"{target_url.split('/')[-1]} downloaded in {around(time() - ptime, decimals=2)} seconds.")
    logging.info(f"{target_url.split('/')[-1]} downloaded in {around(time() - ptime, decimals=2)} seconds.")


# Function for multi-threaded downloading
def multithread_download(target_urls, workers=3):
    # <> We want to kick "troubled" jobs down the road
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
    # <> Refresh the session on attempt.
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


# Get a data product H5 file from laads and return it in some form
def get_product_file(session_obj, url_checksum, write_local=False, return_content=False, return_file=True):
    # Unpack the URL/checksum
    target_url = url_checksum[0]
    checksum = url_checksum[1]

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
            # Get the write path
            write_path = environ["input_files_path"] + target_url.split('/')[-1]
            # Save the file
            with open(write_path, 'wb') as f:
                f.write(r.content)
            # While the checksum is not correct
            while not check_checksum(write_path, checksum):
                # Log this occurrence
                logging.warning(f"Checksum did not match validation for {target_url.split('/')[-1]}. Attempting to redownload.")
                # Send to repeated submission function
                r = try_try_again(r, session_obj, target_url)
        # If content
        if return_content is True:
            return r.content
        # If we are returning the file
        if return_file:
            # <> Need to know if it's HDF4 or HDF5
            # Convert the response content to an H5py File object and return
            h5file = h5py.File(io.BytesIO(r.content), 'r')
            return h5file
        # If returning nothing else, but successfully reached this point, return True
        return True
    # If it fails (incomplete file)
    except Exception:
        # Print and log the exception
        traceback.print_exc()
        # Print a warning
        print(f'Warning: File {target_url} could not be converted to h5. Possibly incomplete.')
        logging.warning(f'Warning: File {target_url} could not be converted to h5. Possibly incomplete.')
        # Return None
        return None


def get_product_ids_urls_checksums(product, archive_set='5000', workers=6):

    # Start time
    stime = time()

    logging.info(f"Starting retrieval of file details for {product}, AS{archive_set}.")

    # Chunk size (number of days to search at a time)
    # <> Optimize chunk size based on a few random tests?
    chunk_size = 50

    # Get the start and end dates for the product / archive set combination
    product_start, product_end = get_product_date_range(product,
                                                        archive_set=archive_set)

    logging.info(f"Overall date range {product_start.isoformat()} to {product_end.isoformat()}.")

    # Task list
    task_list = []

    # Set current date to start
    current_date = product_start

    # While
    while current_date < product_end:
        # Current end date (based on current date and chunk size)
        current_end = current_date + datetime.timedelta(days=chunk_size)
        # Add time window to the task list
        task_list.append((current_date, current_end))
        # Add to current date
        current_date += datetime.timedelta(days=chunk_size + 1)

    logging.info(f"Chunk size {chunk_size} days. Total of {len(task_list)} tasks.")

    # Assemble the path to the file
    output_path = Path(environ["support_files_path"] + f'{archive_set}_{product}_files_' + datetime.datetime.now().strftime("%m%d%Y") + ".json")
    # Main results dictionary
    main_dict = {}
    # Start a ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Submit the tasks from the url list to the worker function
        future_events = {executor.submit(multithread_product_ids_urls_checksums,
                                         date_range, product, archive_set=archive_set): date_range for date_range in
                         task_list}
        # As each worker is finished
        for event in as_completed(future_events):
            # Get the results dictionary
            results_dict = event.result()
            # Merge with the ongoing dictionary
            main_dict = {**main_dict, **results_dict}

    logging.info(f"All file details retrieved in {around(time() - stime, decimals=2)} seconds. Writing output...")

    # When we're finished, save the dictionary
    with open(output_path, 'w') as of:
        json.dump(main_dict, of, indent=4)

    logging.info(f"Output saved to {output_path}.")


def multithread_product_ids_urls_checksums(date_range, product, archive_set='5000'):
    # Start time for worker
    stime = time()
    # Logging
    logging.info(f"Worker retrieving IDs, URLs, and checksums for {product}, AS{archive_set} for {date_range[0].isoformat()} to {date_range[1].isoformat()}.")
    # Base URL
    base_url = "https://ladsweb.modaps.eosdis.nasa.gov"
    # Dictionary for results
    results_dict = {}
    # Make a new, un-authenticated session
    s = requests.Session()
    # Assemble a search based on the current date and chunk size
    product_search = f"/api/v1/files/product={product}&collection={archive_set}&dateRanges={date_range[0].isoformat()}..{date_range[1].isoformat()}"
    # While valid request is false
    while True:
        # Make a request based on the search
        r = s.get(base_url + product_search, allow_redirects=False)
        # Try to parse the response into json
        try:
            file_ids = r.json()
            break
        except:
            logging.warning(f'Request for file IDs from {product_search} failed. Retrying.')

    # For each file ID in the response
    for file_id in r.json():
        # Form the URL to get the file details
        checksum_url = f'https://ladsweb.modaps.eosdis.nasa.gov/api/v1/filePage/useSimple=true&fileId={file_id}'
        # While True
        while True:
            # Get the target url
            checksum_request = s.get(checksum_url, allow_redirects=False)
            # Try and parse the response
            try:
                # File name
                file_name = r.json()[file_id]['name']
                break
            # If the request was not valid
            except:
                logging.warning(f'Request for checksum from {checksum_url} failed. Retrying.')
        # Get file details from the name
        year = file_name.split('.')[1][1:5]
        doy = file_name.split('.')[1][5:8]
        # Retrieve the checksum
        checksum = get_checksum_from_details_page(checksum_request.text, file_name)
        # Form URL
        file_url = environ['laads_alldata_url'] + f'{archive_set}/' + f'{product}/' + f'{year}/' + f'{doy}/' + file_name
        # Add the results to the dictionary
        results_dict[file_id] = {'name': file_name,
                                 'url': file_url,
                                 'checksum': checksum}
    # Logging
    logging.info(
        f"Worker completed retrieval for {product}, AS{archive_set} for {date_range[0].isoformat()} to {date_range[1].isoformat()} in {around(time() - stime, decimals=2)} seconds.")

    # Return the results dictionary
    return results_dict


def get_checksum_from_details_page(html, file_name):
    # Switch for when the checksum will be in the next td
    checksum_next = False
    # Checksum placeholder
    checksum = None
    # Get the root
    root = lh.fromstring(html)
    # For each element in the root
    for element in root.iter():
        # If it's a table
        if element.attrib.get('class') == "table":
            # For each row in the table
            for tr in element:
                # For each division in the row
                for td in tr:
                    # For the text in the division
                    for text in td.itertext():
                        # If this is the checksum
                        if checksum_next:
                            # Try converting checksum to integer
                            try:
                                int(text)
                                # Return it
                                return text
                            except:
                                logging.error(f"Checksum {text} is not a valid checksum for {file_name}.")
                        # If the text reads "Checksum"
                        if text == "Checksum":
                            # Flip the checksum next switch
                            checksum_next = True


# Function to return a dictionary of URLs to a data product on LAADS (will update existing)
def get_product_availability(data_product,
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

# Get the start and end dates for a product in an archive set
def get_product_date_range(product, archive_set='5000'):

    # Get an authenticated session
    s = connect_to_laads()
    # Base URL for product/archive set
    url = environ['laads_alldata_url'] + archive_set + '/' + product
    # Request the json for the product/archive set
    r = s.get(url + '.json')

    year_list = []

    for year_dict in r.json():
        year_list.append(year_dict['name'])
    sorted_years = sorted(year_list, key=int)

    # Get the days for the earliest year
    r = s.get(url + f'/{sorted_years[0]}.json')

    doy_list = []

    for doy_dict in r.json():
        doy_list.append(doy_dict['name'])

    sorted_doys = sorted(doy_list, key=int)
    earliest_doy = sorted_doys[0]

    # Get the days for the latest year
    r = s.get(url + f'/{sorted_years[-1]}.json')

    doy_list = []

    for doy_dict in r.json():
        doy_list.append(doy_dict['name'])

    sorted_doys = sorted(doy_list, key=int)
    latest_doy = sorted_doys[-1]

    start_date = datetime.date(year=int(sorted_years[0]),
                               month=1,
                               day=1) + datetime.timedelta(days=int(earliest_doy))

    end_date = datetime.date(year=int(sorted_years[-1]),
                             month=1,
                             day=1) + datetime.timedelta(days=int(latest_doy))

    return start_date, end_date


# Get a list of urls corresponding to the file details
def get_file_detail_urls(target_url):
    # Get the target url
    r = requests.get(target_url)
    # Retrieve the text
    html = r.text
    # Set the string of the html as the root
    root = lh.fromstring(html)
    # List for tuples
    info = []
    # Retrieve the table
    # <> Try except this search to see if you are at the right depth in LAADS archive
    try:
        table = root.get_element_by_id("laads-archive-list")
    except:
        print(f"No valid laads-archive-list found in html at {target_url}.")
        logging.warning(f"No valid laads-archive-list found in html at {target_url}.")
        # Return empty list
        return info
    # For each row in the table
    for tr in table:
        # For each element in the row
        for element in tr.iter():
            # If the tag of the element is 'a'
            if element.tag != 'a':
                # Skip it
                continue
            # Get the element's title
            title = element.attrib.get('title')
            # If there is no title
            if title is None:
                # Skip it
                continue
            # If the title does not contain the file details phrase
            if "View file details for" not in title:
                # Skip it
                continue
            # If we get this far, extract file name
            file = title.replace('View file details for ', '')
            # Get the link (href)
            href = element.attrib.get("href")
            # Store the href with the file name
            info.append((file, href))
    # Return the list
    return info


# Get a list of urls to query for checksums from a URL dictionary
def get_checksum_urls(url_dict, data_product, archive_set='5000'):
    # Dictionary for the checksum urls
    checksum_dict = {}
    # List for the checksum urls
    checksum_list = []
    # For each spatial location in the url dictionary
    for location in url_dict.keys():
        # For each year in the spatial location
        for year in url_dict[location].keys():
            # For each for doy in the year
            for doy in url_dict[location][year].keys():
                # If the year is not in the checksum dict
                if year not in checksum_dict.keys():
                    # Add it with a list
                    checksum_dict[year] = []
                # If the doy is not in the year
                if doy not in checksum_dict.get(year):
                    # Add the doy
                    checksum_dict[year].append(doy)
    # For each year in the checksum dictionary
    for year in checksum_dict.keys():
        # For each doy in the year
        for doy in checksum_dict.get(year):
            # Assemble the URL
            checksum_url = environ['laads_alldata_url'] + archive_set
            checksum_url += f'/{data_product}'
            checksum_url += f'/{year}/{doy}'
            # Add to list
            checksum_list.append(checksum_url)
    # Return the list
    return checksum_list


# Takes input of URL to LAADS page where files have details (typically inside a DOY)
def get_checksums(target_url, existing_dict=None):
    # Get the hrefs for the file details
    info_list = get_file_detail_urls(target_url)
    # If there is no existing dictionary
    if not existing_dict:
        # Make empty dictionary
        existing_dict = {}
    # For each file
    for file in info_list:
        # Get the checksum
        checksum = get_checksum(file[1])
        # If there is a checksum
        if checksum:
            # Store in the dict
            existing_dict[file[0]] = get_checksum(file[1])
        # Otherwise (no valid checksum)
        else:
            # Back-off timer
            back_off = 5
            # While there is no valid checksum
            while not checksum:
                # Wait a hot second
                sleep(back_off)
                # Get the checksum
                checksum = get_checksum(file[1])
                # Add to back off timer
                back_off += 1
    # Return dictionary
    return existing_dict


def get_checksum(details_href):
    # Assemble the url
    target_url = "https://ladsweb.modaps.eosdis.nasa.gov" + details_href
    # Get the target url
    r = requests.get(target_url)
    # Retrieve the text
    html = r.text
    # Switch for when the checksum will be in the next td
    checksum_next = False
    # Get the root
    root = lh.fromstring(html)
    # For each element in the root
    for element in root.iter():
        # If it's a table
        if element.attrib.get('class') == "table":
            # For each row in the table
            for tr in element:
                # For each division in the row
                for td in tr:
                    # For the text in the division
                    for text in td.itertext():
                        # If this is the checksum
                        if checksum_next:
                            # Try converting checksum to integer
                            try:
                                int(text)
                                # Return it
                                return text
                            except:
                                logging.error(f"Checksum {text} is not a valid checksum.")
                        # If the text reads "Checksum"
                        if text == "Checksum":
                            # Flip the checksum next switch
                            checksum_next = True
    # If we get here, we did not find a checksum
    logging.warning(f"Valid checksum not found for {target_url}.")


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


# Based on https://pastebin.com/cKATyGLb
def check_checksum(file_path, checksum):

    # Convert checksum to int
    checksum = int(checksum)

    # Establish hex conversions (0-255 range)
    crctab = [0x00000000, 0x04c11db7, 0x09823b6e, 0x0d4326d9, 0x130476dc,
              0x17c56b6b, 0x1a864db2, 0x1e475005, 0x2608edb8, 0x22c9f00f,
              0x2f8ad6d6, 0x2b4bcb61, 0x350c9b64, 0x31cd86d3, 0x3c8ea00a,
              0x384fbdbd, 0x4c11db70, 0x48d0c6c7, 0x4593e01e, 0x4152fda9,
              0x5f15adac, 0x5bd4b01b, 0x569796c2, 0x52568b75, 0x6a1936c8,
              0x6ed82b7f, 0x639b0da6, 0x675a1011, 0x791d4014, 0x7ddc5da3,
              0x709f7b7a, 0x745e66cd, 0x9823b6e0, 0x9ce2ab57, 0x91a18d8e,
              0x95609039, 0x8b27c03c, 0x8fe6dd8b, 0x82a5fb52, 0x8664e6e5,
              0xbe2b5b58, 0xbaea46ef, 0xb7a96036, 0xb3687d81, 0xad2f2d84,
              0xa9ee3033, 0xa4ad16ea, 0xa06c0b5d, 0xd4326d90, 0xd0f37027,
              0xddb056fe, 0xd9714b49, 0xc7361b4c, 0xc3f706fb, 0xceb42022,
              0xca753d95, 0xf23a8028, 0xf6fb9d9f, 0xfbb8bb46, 0xff79a6f1,
              0xe13ef6f4, 0xe5ffeb43, 0xe8bccd9a, 0xec7dd02d, 0x34867077,
              0x30476dc0, 0x3d044b19, 0x39c556ae, 0x278206ab, 0x23431b1c,
              0x2e003dc5, 0x2ac12072, 0x128e9dcf, 0x164f8078, 0x1b0ca6a1,
              0x1fcdbb16, 0x018aeb13, 0x054bf6a4, 0x0808d07d, 0x0cc9cdca,
              0x7897ab07, 0x7c56b6b0, 0x71159069, 0x75d48dde, 0x6b93dddb,
              0x6f52c06c, 0x6211e6b5, 0x66d0fb02, 0x5e9f46bf, 0x5a5e5b08,
              0x571d7dd1, 0x53dc6066, 0x4d9b3063, 0x495a2dd4, 0x44190b0d,
              0x40d816ba, 0xaca5c697, 0xa864db20, 0xa527fdf9, 0xa1e6e04e,
              0xbfa1b04b, 0xbb60adfc, 0xb6238b25, 0xb2e29692, 0x8aad2b2f,
              0x8e6c3698, 0x832f1041, 0x87ee0df6, 0x99a95df3, 0x9d684044,
              0x902b669d, 0x94ea7b2a, 0xe0b41de7, 0xe4750050, 0xe9362689,
              0xedf73b3e, 0xf3b06b3b, 0xf771768c, 0xfa325055, 0xfef34de2,
              0xc6bcf05f, 0xc27dede8, 0xcf3ecb31, 0xcbffd686, 0xd5b88683,
              0xd1799b34, 0xdc3abded, 0xd8fba05a, 0x690ce0ee, 0x6dcdfd59,
              0x608edb80, 0x644fc637, 0x7a089632, 0x7ec98b85, 0x738aad5c,
              0x774bb0eb, 0x4f040d56, 0x4bc510e1, 0x46863638, 0x42472b8f,
              0x5c007b8a, 0x58c1663d, 0x558240e4, 0x51435d53, 0x251d3b9e,
              0x21dc2629, 0x2c9f00f0, 0x285e1d47, 0x36194d42, 0x32d850f5,
              0x3f9b762c, 0x3b5a6b9b, 0x0315d626, 0x07d4cb91, 0x0a97ed48,
              0x0e56f0ff, 0x1011a0fa, 0x14d0bd4d, 0x19939b94, 0x1d528623,
              0xf12f560e, 0xf5ee4bb9, 0xf8ad6d60, 0xfc6c70d7, 0xe22b20d2,
              0xe6ea3d65, 0xeba91bbc, 0xef68060b, 0xd727bbb6, 0xd3e6a601,
              0xdea580d8, 0xda649d6f, 0xc423cd6a, 0xc0e2d0dd, 0xcda1f604,
              0xc960ebb3, 0xbd3e8d7e, 0xb9ff90c9, 0xb4bcb610, 0xb07daba7,
              0xae3afba2, 0xaafbe615, 0xa7b8c0cc, 0xa379dd7b, 0x9b3660c6,
              0x9ff77d71, 0x92b45ba8, 0x9675461f, 0x8832161a, 0x8cf30bad,
              0x81b02d74, 0x857130c3, 0x5d8a9099, 0x594b8d2e, 0x5408abf7,
              0x50c9b640, 0x4e8ee645, 0x4a4ffbf2, 0x470cdd2b, 0x43cdc09c,
              0x7b827d21, 0x7f436096, 0x7200464f, 0x76c15bf8, 0x68860bfd,
              0x6c47164a, 0x61043093, 0x65c52d24, 0x119b4be9, 0x155a565e,
              0x18197087, 0x1cd86d30, 0x029f3d35, 0x065e2082, 0x0b1d065b,
              0x0fdc1bec, 0x3793a651, 0x3352bbe6, 0x3e119d3f, 0x3ad08088,
              0x2497d08d, 0x2056cd3a, 0x2d15ebe3, 0x29d4f654, 0xc5a92679,
              0xc1683bce, 0xcc2b1d17, 0xc8ea00a0, 0xd6ad50a5, 0xd26c4d12,
              0xdf2f6bcb, 0xdbee767c, 0xe3a1cbc1, 0xe760d676, 0xea23f0af,
              0xeee2ed18, 0xf0a5bd1d, 0xf464a0aa, 0xf9278673, 0xfde69bc4,
              0x89b8fd09, 0x8d79e0be, 0x803ac667, 0x84fbdbd0, 0x9abc8bd5,
              0x9e7d9662, 0x933eb0bb, 0x97ffad0c, 0xafb010b1, 0xab710d06,
              0xa6322bdf, 0xa2f33668, 0xbcb4666d, 0xb8757bda, 0xb5365d03,
              0xb1f740b4]

    UNSIGNED = lambda n: n & 0xffffffff

    buffer = open(file_path, 'rb').read()

    n = len(buffer)
    i = c = s = 0
    for c in buffer:
        tabidx = (s >> 24) ^ c
        s = UNSIGNED((s << 8)) ^ crctab[tabidx]
    while n:
        c = n & 0o0377
        n = n >> 8
        s = UNSIGNED(s << 8) ^ crctab[(s >> 24) ^ c]

    if UNSIGNED(~s) != checksum:
        print(f"Checksums do not match: {UNSIGNED(~s)} vs. {checksum}.")
        return False

    # If we get this far (checksums match) return True
    return True


def main():

    # Set the logging config
    logging.basicConfig(filename=environ['log_files_path'] + f'{datetime.datetime.now():%Y%m%d%H%M%S}.log',
                        filemode='w',
                        format=' %(levelname)s - %(asctime)s - %(message)s',
                        level=logging.INFO)

    laads_dict = LaadsUrlsDict('MCD43D31', archive_set='6')



if __name__ == '__main__':

    main()



