import requests
import json
from time import sleep
import datetime
import dotenv
from os import environ, walk, mkdir
from os.path import exists
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from numpy import around
from time import time
import logging
import lxml.html as lh
from shutil import rmtree

# Load the .env file
dotenv.load_dotenv()


# Class LAADS data set (to load when you need it)
class LAADsDataSet:

    def __init__(self,
                 name,
                 archive_set=None,
                 product=None,
                 start_date=None,
                 end_date=None,
                 include=None,
                 exclude=None,
                 update_catalog=False,
                 remake_catalog=False):

        self.name = name
        self.archive_set = archive_set
        self.product = product
        self.start_date = start_date
        self.end_date = end_date
        # Portions of file names to include or exclude
        self.include = listify(include)
        self.exclude = listify(exclude)

        # Dictionaries for indexing the files
        self.by_date = {}
        self.by_filename = {}
        self.by_year_doy = {}

        # Spin up the object
        self.spinup(archive_set,
                    product,
                    start_date,
                    end_date,
                    update_catalog,
                    remake_catalog)

    # Spinup procedure for the object
    def spinup(self, archive_set, product, start_date, end_date, update_catalog, remake_catalog):

        # LOAD SPECIFICATION FILE, OR CREATE ONE
        # If there is a dataset file with the name
        if self.find_dataset_spec():
            for attribute, attr_name in zip([archive_set, product, start_date, end_date],
                                            ['archive_set', 'product', 'start_date', 'end_date']):
                # Check the specification for an existing value and compare to provided value
                self.check_spec(attribute, attr_name)
        # Otherwise (no dataset spec)
        else:
            # If there is no archive set or product
            if not self.archive_set or not self.product:
                # Log an error
                logging.error(f"No preexisting specification found for LAADsDataSet {self.name}. Provide at least "
                              f"archive_set and product on instantiation.")
                # Return
                return
            # Convert dates to strings
            if self.start_date:
                start_date = self.start_date.strftime('%m/%d/%Y')
            if self.end_date:
                end_date = self.end_date.strftime('%m/%d/%Y')
            # Form output dictionary
            output_dict = {'Name': self.name,
                           'Archive Set': self.archive_set,
                           'Product': self.product,
                           'Start Date': start_date,
                           'End Date': end_date,
                           'Include': self.include,
                           'Exclude': self.exclude}
            # Save the specification
            with open(Path(environ["support_files_path"], f"{self.name}_dataset_spec.json"), mode='w') as of:
                json.dump(output_dict, of, indent=4)

        # CATALOG FILES: LOAD, UPDATE, OR CREATE NEW ONE
        # Get the date of the latest catalog (or None if there is none)
        catalog_date = self.find_catalog_file()
        # If remake of catalog file is requested (remake from scratch) or there is no catalog
        if remake_catalog or not catalog_date:
            # Make a new catalog
            get_catalog(self, workers=5)
        # Otherwise (existing catalog)
        else:
            # Ingest catalog file
            self.ingest_catalog_file(catalog_date)
            # If the catalog is to be updated
            if update_catalog:
                # Update the existing catalog
                pass

    # Check the specification for a given value
    def check_spec(self, attribute, attr_name):
        # If an attribute value was provided
        if attribute:
            # If the loaded archive set differed from the specified
            if getattr(self, attr_name) != attribute:
                # Log a warning
                logging.warning(f"For LAADsDataSet {self.name}, {attr_name} {attribute} was specified,"
                                f" but {attr_name} {attribute} was found in a preexisting"
                                f" dataset specification and was used.")

    # Look for a dataset specification file
    def find_dataset_spec(self):
        # Walk the support file directory
        for root, dirs, files in walk(environ["support_files_path"]):
            # For each file name
            for name in files:
                # If the dataset's name is in the file name
                if f"{self.name}_dataset_spec.json" in name:
                    # Open the file
                    with open(Path(root, name), 'r') as f:
                        # Load dictionary
                        dataset_dict = json.load(f)
                    # Transfer the values
                    self.archive_set = dataset_dict['Archive Set']
                    self.product = dataset_dict['Product']
                    # If there is a start date
                    if dataset_dict['Start Date']:
                        self.start_date = datetime.datetime.strptime(dataset_dict['Start Date'], '%m/%d/%Y').date()
                    # If there is an end date
                    if dataset_dict['End Date']:
                        self.end_date = datetime.datetime.strptime(dataset_dict['End Date'], '%m/%d/%Y').date()
                    self.include = dataset_dict['Include']
                    self.exclude = dataset_dict['Exclude']
                    # Return True
                    return True
        # Return False (didn't find a file)
        return False

    # Find the latest date for a support file containing a given string
    def get_latest_support_file_date(self, file_str):
        # Latest date variable
        latest_date = None
        # Walk the support file directory
        for root, dirs, files in walk(environ["support_files_path"]):
            # For each file name
            for name in files:
                # If the dataset's name is in the file name
                if file_str in name:
                    # Split the name
                    split_name = name.split('_')
                    # Make a datetime date object from the name
                    file_date = datetime.datetime.strptime(split_name[-1].replace('.json', ''), '%m%d%Y').date()
                    # If there is no latest date yet
                    if not latest_date:
                        # Set the file's date
                        latest_date = file_date
                    # Otherwise, if the file's date is later
                    elif file_date > latest_date:
                        # Set the file's date as latest
                        latest_date = file_date
            # Return latest date
            return latest_date

    # Find the date of latest dataset catalog file
    def find_catalog_file(self):
        return self.get_latest_support_file_date(f"{self.name}_catalog_")

    # Ingest a catalog file
    def ingest_catalog_file(self, catalog_date):
        # If the date provided is a datetime.date object
        if isinstance(catalog_date, datetime.date):
            # Convert to string
            catalog_date = catalog_date.strftime('%m%d%Y')
        # Assemble path
        catalog_path = Path(environ['support_files_path'], f'{self.name}_catalog_{catalog_date}.json')
        # Open the file
        with open(catalog_path, mode='r') as f:
            # Convert to json
            incoming_catalog = json.load(f)
        # For each key (File name) in the dictionary
        for filename in incoming_catalog.keys():
            # Instantiate an object
            file_obj = LAADSFile(filename,
                                 incoming_catalog[filename]['date'],
                                 incoming_catalog[filename]['checksum'])
            # Store under the default indexing dictionaries
            self.by_filename[filename] = file_obj
            # Add sublist for date object key
            if file_obj.date not in self.by_date.keys():
                self.by_date[file_obj.date] = []
            self.by_date[file_obj.date].append(file_obj)
            # Get year and doy keys
            year_key = str(file_obj.date.year)
            doy_key = get_doy_from_date(file_obj.date,
                                        zero_pad_digits=3)
            # Store by year and doy
            if year_key not in self.by_year_doy.keys():
                self.by_year_doy[year_key] = {}
            if doy_key not in self.by_year_doy[year_key].keys():
                self.by_year_doy[year_key][doy_key] = []
            self.by_year_doy[year_key][doy_key].append(file_obj)

    # Find the latest download file
    def find_download_file(self):
        return self.get_latest_support_file_date(f"{self.name}_download_")

    # Download the whole catalog
    def download_catalog(self, from_scratch=False):
        # Directory to download to
        download_dir = Path(environ['input_files_path'], self.name)
        # If there is a directory to store the files
        if exists(download_dir):
            # If starting from scratch
            if from_scratch:
                # Remove the directory
                rmtree(download_dir)
                # Remake the directory
                mkdir(download_dir)
        # Otherwise (no directory)
        else:
            # Make the directory
            mkdir(download_dir)
        # Attempt to get a date of a download file (None if none)
        download_file_date = self.find_download_file()
        # If there was a download file
        if download_file_date:
            # Open it
            with open(Path(environ['support_files_path'], f'{self.name}_download_{download_file_date}.json'), mode='r') as f:
                # Load it
                download_json = json.load(f)
        # Otherwise (no download file)
        else:
            # Make an empty dictionary
            download_json = {}
        # Task list
        task_list = []
        # For each date
        for date_key in self.by_date:
            # Get the date components
            year = str(date_key.year)
            doy = get_doy_from_date(date_key, zero_pad_digits=3)
            # For each file in the list for DOY
            for file in self.by_date[date_key]:
                # Full URL for file
                file_url = environ['laads_alldata_url'] + f'{self.archive_set}' + \
                           f'/{self.product}' + f'/{year}' + f'/{doy}' + f'/{file.name}'
                # If the file is already downloaded
                if exists(Path(download_dir, file.name)):
                    # If the file is in the previously problematic files
                    if file.name in download_json['Failed Files']:
                        # Add to task list
                        task_list.append((file_url, Path(download_dir, file.name), file.checksum))
                # Otherwise (not downloaded)
                else:
                    # Add to task list
                    task_list.append((file_url, Path(download_dir, file.name), file.checksum))
        # Log information about the task list
        logging.info(f'Sending {len(task_list)} files to multithreaded download.')
        # Mark start time
        stime = time()
        # Start a ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=3) as executor:
            # Submit the tasks from the target list to the worker function, along with an authenticated session
            future_events = {executor.submit(download_laads_file,
                                             get_laads_session(),
                                             target): target for target in task_list}
        # Report on the overall time taken
        logging.info(f"All downloads finished in {around(time() - stime, decimals=2)} seconds.")


# Class for LAADsFile
class LAADSFile:

    def __init__(self, file_name, date, checksum, local_path=None):

        self.name = file_name
        self.date = date
        self.checksum = checksum
        self.local_path = local_path

        # If the date is a string
        if isinstance(self.date, str):
            # Convert to a datetime date object
            self.date = datetime.datetime.strptime(self.date, '%m%d%Y').date()

    # Flatten this object for saving
    def flatten(self):
        # Output dictionary
        output_dict = {}
        # For each attribute
        for attribute in self.__dict__.keys():
            # If it's the date
            if attribute == 'date':
                # Convert to string and store
                output_dict[attribute] = getattr(self, attribute).strftime('%m%d%Y')
            # Otherwise (not the date)
            else:
                # Store it
                output_dict[attribute] = getattr(self, attribute)
        # Return the output dictionary
        return output_dict

    # Flatten the lightest possible version of this object for saving
    def flatten_light(self):

        return {'date': self.date.strftime('%m%d%Y'),
                'checksum': self.checksum}

    # Get the year and DOY from date
    def get_year_doy(self):
        # Return year and DOY
        return self.date.year, (self.date - datetime.date(year=self.date.year, month=1, day=1)).days + 1


# Listify a target (return an empty list if None, encapsulate in list if single value)
def listify(target):
    # If the input is None
    if not target:
        # Return an empty list
        return []
    # Otherwise, if input is not a list
    elif not isinstance(target, list):
        # Return input encapsulated in list
        return [target]
    # Otherwise (it was a list)
    else:
        # Return input unchanged
        return target


# Download an input file (store it locally). Target is EarthDataFileRequest object
def download_laads_file(session_obj, target, attempt_limit=2):
    # Split out the target URL and destination
    url = target[0]
    destination = target[1]
    checksum = target[2]

    # Attempt count
    attempt_count = 0
    logging.debug(f"Heading into while loop in download function for {url}")

    # While True
    while True:
        # Increment attempt count
        attempt_count += 1
        # If we have exceeded the attempt count
        if attempt_count > attempt_limit:
            # Log an error
            logging.error(f'Downloading file from {url} failed after {attempt_limit} of {attempt_limit} attempts.')
            # Return False (failed)
            return False
        # Log the info
        logging.info(f'Attempting to download {url}. Attempt {attempt_count}.')
        # Make a request for the provided URL
        r = ask_nicely(session_obj, url)
        # If the request failed
        if not r:
            # Log an error
            logging.error(f'A valid request could not be made for {url}.')
            # Skip the loop
            continue
        # Try to write the content of the request to storage, but if it returns False (failed)
        if not write_request_content(r, destination):
            # Log an error
            logging.error(f'The downloaded file could not be written to {destination}.')
            # Skip the loop
            continue
        # If a checksum was provided
        if checksum:
            # Check the checksum. If it returns False (failed)
            if not check_checksum(destination, checksum):
                # Log an error
                logging.error(f'The checksum for {url} did not match the content written to {destination}.')
                # Skip the loop
                continue

        # If we made it this far, we have succeeded. Return True.
        return True


# Function for multi-threaded downloading
def multithread_download(targets, workers=3):
    # Mark start time
    stime = time()
    # Guard against single URLs submitted
    if not isinstance(targets, list):
        # Encapsulate in list
        targets = [targets]
    # Start a ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Submit the tasks from the target list to the worker function, along with an authenticated session
        future_events = {executor.submit(download_earthdata_file,
                                         get_laads_session(),
                                         target): target for target in targets}
    # Report on the overall time taken
    logging.info(f"All downloads finished in {around(time() - stime, decimals=2)} seconds.")


# Ensure a path exists
def ensure_file_path_dirs_exist(file_path):
    # Empty check path
    check_path = Path()
    # For each part in the file path (apart from the file name)
    for part in file_path.parts[0:-1]:
        # Form the path
        check_path = Path(str(check_path), part)
        # If this section of the path does not exists
        if not exists(check_path):
            # Log the info
            logging.info(f'Creating {str(check_path)} for write path {str(file_path)}')
            # Try to make the directory
            try:
                mkdir(check_path)
            # If this does not work
            except:
                # Log error
                logging.error(f'Could not create directory {check_path} for {file_path}.')
                # Return False
                return False
    # Return True
    return True


# Write content of request to storage
def write_request_content(request, write_path):
    # If the write path already exists
    if exists(write_path):
        # Log a warning
        logging.warning(f'Already a file at {write_path}. Overwriting.')
    # Ensure the path, but if it returns False (failed)
    if not ensure_file_path_dirs_exist(write_path):
        # Log an error
        logging.error(f'Could not ensure write path for {write_path}.')
        # Return False (failed)
        return False
    # Log the info
    logging.info(f'Writing request content to {write_path}.')

    # Try to write the request content
    try:
        with open(write_path, 'wb') as f:
            f.write(request.content)
    except:
        # Log error
        logging.error(f'Could not write request content to {write_path}.')
        # Return False (failed)
        return False
    else:
        # Log the info
        logging.info(f'Successfully wrote request content to {write_path}.')
        # Return True (succeeded)
        return True


# Get a laads session via token-based authorization
def get_laads_session():
    # Header command utilizing security token
    authToken = {'Authorization': f'Bearer {environ["laads_token"]}'}
    # Create session
    s = requests.session()
    # Update header with authorization
    s.headers.update(authToken)
    # Return the session object
    return s


# Get a brand new catalog based on a LAADSDataSet object
def get_catalog(dataset, workers=5):

    # Start time
    stime = time()

    # Log start of catalog
    logging.info(f"Starting retrieval of catalog for {dataset.product}"
                 f" from archive set {dataset.archive_set}.")

    # Start a LAADs session
    s = get_laads_session()

    # Get the product year json
    years_json = get_years_json(s, dataset)

    # Chunk size (number of days to search at a time)
    chunk_size = 50

    # WE WILL CRAWL THE YEARS LINEARLY TO GET A COMPLETE LIST OF THE ALL THE DAY URLS TO BE CRAWLED.

    # Task list
    task_list = []

    # Chunk list
    chunk = []

    # For each year in the json
    for year in years_json['content']:
        # Reference year name
        year_name = year['name']
        try:
            curr_date = datetime.date(year=(int(year_name)),
                                      day=1,
                                      month=1)
        except:
            logging.warning(f'Year {year_name} in product {dataset.product}'
                            f' in archive set {dataset.product} is not a valid year.')
            # Skip the year
            continue
        # If the dataset has a specified start date
        if dataset.start_date:
            # If the year is before the start date year
            if int(year_name) < dataset.start_date.year:
                # Skip the year
                continue
        # If the dataset has a specified end date
        if dataset.end_date:
            # If the year is after the end date year
            if int(year_name) > dataset.start_date.year:
                # Skip the year
                continue
        # If it's a directory (contains DOYs)
        if year['resourceType'] == 'Directory':
            # Attempt to get the JSON containing Days of Year for the year
            doys_json = get_laads_json(s, environ['laads_base_url'] + year['self'] + '.json')
            # For each doy in the json
            for doy in doys_json['content']:
                # If it's a directory (contains files)
                if doy['resourceType'] == 'Directory':
                    # If the dataset has a specified start date
                    if dataset.start_date:
                        # If the doy is before the start date
                        if get_dateobj_from_yeardoy(int(year_name), int(doy['name'])) < dataset.start_date:
                            # Skip the DOY
                            continue
                    # If the dataset has a specified end date
                    if dataset.end_date:
                        # If the doy is after the end date
                        if get_dateobj_from_yeardoy(int(year_name), int(doy['name'])) > dataset.end_date:
                            # Skip the DOY
                            continue
                    # Add the year, doy, and full URL to the chunk
                    chunk.append([year['name'],
                                  doy['name'],
                                  environ['laads_base_url'] + doy['self'] + '.json'])
                    # If the chunk has reached appropriate size
                    if len(chunk) == chunk_size:
                        # Add the chunk to the task list
                        task_list.append(chunk)
                        # Empty the chunk
                        chunk = []
    # Log information
    logging.info(f"Total of {len(task_list)} chunks of size {chunk_size} days entering multithreading.")

    # Main results dictionary
    main_dict = {}
    # Start a ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Submit the tasks from the url list to the worker function
        future_events = {executor.submit(mt_get_files_for_catalog,
                                         chunk): chunk for chunk in task_list}
        # As each worker is finished
        for event in as_completed(future_events):
            # Get the results dictionary
            results_dict = event.result()
            # Merge with the ongoing dictionary
            main_dict = {**main_dict, **results_dict}

    logging.info(f"File catalog for {dataset.product} in archive set {dataset.archive_set}"
                 f" retrieved in {around(time() - stime, decimals=2)} seconds. Writing output...")

    # Assemble the path to the catalog file
    output_path = Path(environ["support_files_path"] + f"{dataset.name}_catalog_" + datetime.datetime.now().strftime(
        "%m%d%Y") + ".json")

    # When we're finished, save the dictionary
    with open(output_path, 'w') as of:
        json.dump(main_dict, of, indent=4)

    logging.info(f"Output saved to {output_path}.")


# Get the json for years
def get_years_json(session, dataset):
    # Assemble URL
    years_url = environ['laads_alldata_url'] + f'/{dataset.archive_set}' + f'/{dataset.product}.json'
    # Get json from LAADs (will return None if fails)
    return get_laads_json(session, years_url)


def get_laads_json(session,
                   url,
                   back_off_base=0,
                   back_off_inc=5,
                   attempts_per_session=1,
                   max_attempts=2):
    # Session attempts
    session_attempts = attempts_per_session
    # Attempts count
    attempts = 0
    # While valid request is false
    while True:
        # Increment attempts
        attempts += 1
        # Ask nicely for the json
        r = ask_nicely(session, url)
        # If we got a response
        if r:
            # Try to parse the response into json
            try:
                response_json = r.json()
                # If successful, return the JSON
                return response_json
            # If not successful
            except:
                logging.debug(f'Request response content for {url} was not a valid JSON.'
                              f'Attempt {attempts} of {max_attempts}. Retrying.')
        # If attempt max has been reached
        if attempts == max_attempts:
            # Break the loop
            break
        # Add to the back-off timer
        back_off_base += back_off_inc
        # Wait quietly and politely
        sleep(back_off_base)
        # Decrement remaining attempts for session
        session_attempts -= 1
        # If session attempts has reached 0
        if session_attempts == 0:
            # Refresh the session
            session = get_laads_session()
            # Reset the attempts
            session_attempts = attempts_per_session
    # If we reach this far, we failed.
    logging.error(f'Request for JSON {url} failed after {max_attempts} attempts.')
    # Return None
    return None


def ask_nicely(session,
               url,
               back_off_base=0,
               back_off_inc=1,
               attempts_per_session=3,
               max_attempts=10):
    # Attempts count
    attempts = 0
    # Attempts left in session
    session_attempts = attempts_per_session
    # While attempts continue
    while True:
        # Increment attempts
        attempts += 1
        # Make a request
        r = session.get(url, allow_redirects=False)
        # If the request appears successful (code 200)
        if r.status_code == 200:
            # Return the request
            return r
        # Log a non-200 status code
        logging.debug(f'Request for {url}, attempt {attempts} returned code: {r.status_code}.')
        # If attempt max has been reached
        if attempts == max_attempts:
            # Break the loop
            break
        # Add to the back-off timer
        back_off_base += back_off_inc
        # Wait quietly and politely
        sleep(back_off_base)
        # Decrement remaining attempts for session
        session_attempts -= 1
        # If session attempts has reached 0
        if session_attempts == 0:
            # Refresh the session
            session = get_laads_session()
            # Reset the attempts
            session_attempts = attempts_per_session
    # If we reach this far, we failed.
    logging.error(f'Request for {url} failed after {max_attempts} attempts. Latest code: {r.status_code}.')
    # Return None
    return None


def mt_get_files_for_catalog(chunk):
    # Start time for worker
    stime = time()
    # Results dictionary
    results_dict = {}
    # Make a session
    s = get_laads_session()
    # For each doy in the chunk
    for doy in chunk:
        # Ask nicely for the json
        doy_json = get_laads_json(s, doy[2])
        # If we got it, it didn't fail
        if doy_json:
            # For each file in the json
            for file in doy_json['content']:
                # Make an object
                file_obj = LAADSFile(file['name'],
                                     get_dateobj_from_yeardoy(doy[0], doy[1]),
                                     file['cksum'])
                # File information into dictionary, flattening the object for saving
                results_dict[file['name']] = file_obj.flatten_light()
    # Return the results dictionary
    return results_dict


def get_dateobj_from_yeardoy(year, doy):
    # Convert year and doy to ints
    year = int(year)
    doy = int(doy)
    # Return the datetime object
    return datetime.date(year=year, month=1, day=1) + datetime.timedelta(days=doy - 1)


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
def get_doy_from_date(date, zero_pad_digits=None):
    # Get the day of year
    doy = (date - datetime.date(year=date.year, month=1, day=1)).days + 1
    # If there is a zero pad requested
    if zero_pad_digits:
        # Zero pad the number
        doy = zero_pad_number(doy, zero_pad_digits)
    # Return the doy
    return doy


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
        logging.info(f"Checksums did not match for {file_path}: {UNSIGNED(~s)} vs. {checksum}.")
        return False

    # If we get this far (checksums match) return True
    logging.info(f"Checksums matched for {file_path}.")
    return True


def main():

    # Set the logging config
    logging.basicConfig(filename=environ['log_files_path'] + f'{datetime.datetime.now():%Y%m%d%H%M%S}.log',
                        filemode='w',
                        format=' %(levelname)s - %(asctime)s - %(message)s',
                        level=logging.DEBUG)

    dataset = LAADsDataSet('Test001')

    dataset.download_catalog()

if __name__ == '__main__':

    main()



