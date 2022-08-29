import requests
import h5py
import io
from os import environ
from dotenv import load_dotenv
import logging
import datetime
from pathlib import Path
from time import time
from numpy import around
import json

# Load environmental variables
load_dotenv()

# Connect to LAADS and return a session object
def connect_to_laads():
    # Get EarthData token
    ed_token = environ['laads_token']
    # Header command utilizing security token
    authToken = {'Authorization': f'Bearer {ed_token}'}
    # Create session
    s = requests.session()
    # Update header with authorization
    s.headers.update(authToken)
    # Return the session object
    return s


# Get a VIIRS H5 file from laads and return it in some form
def get_VIIRS_file(session_obj, target_url):

    # Start time
    stime = time()

    logging.info(f"Making request for {target_url}...")

    # Request the H5 file from the provided URL
    r = session_obj.get(target_url)

    logging.info(f"Request returned with code {r.status_code} in {around(time() - stime, decimals=2)} seconds.")
    logging.info(f"Length of content is {around(len(r.content) / 1e6, decimals=2)} megabytes.")
    logging.info(f"Attempting to convert content to H5 file...")

    # Checkpoint time
    ptime = time()

    try:
        h5file = h5py.File(io.BytesIO(r.content), 'r')
        logging.info(f"Conversion to H5 file successful in {around(time() - ptime, decimals=2)} seconds.")
    except:
        logging.warning(f"Conversion to H5 file unsuccessful in {around(time() - ptime, decimals=2)} seconds.")

    # Checkpoint time
    ptime = time()

    write_path = Path("F:/UMB/testfile.hdf")

    logging.info(f"Attempting to write file...")

    try:
        with open(write_path, 'wb') as f:
            f.write(r.content)
        logging.info(f"Writing of content to file successful in {around(time() - ptime, decimals=2)} seconds.")
    except:
        logging.warning(f"Writing of content to file unsuccessful in {around(time() - ptime, decimals=2)} seconds.")

    # Checkpoint time
    ptime = time()

    logging.info(f"Attempting to read file...")

    try:
        h5file = h5py.File(write_path, 'r')
        logging.info(f"Reading file successful in {around(time() - ptime, decimals=2)} seconds.")
    except:
        logging.warning(f"Reading file unsuccessful in {around(time() - ptime, decimals=2)} seconds.")

    logging.info(f"Test completed in a total of {around(time() - stime, decimals=2)} seconds.")

    logging.debug(f"Header: {json.dumps(dict(r.headers), indent=4)}")


def main():
    # Set the logging config
    logging.basicConfig(filename=environ['log_files_path'] + f'{datetime.datetime.now():%Y%m%d%H%M%S}_tokentest.log',
                        filemode='w',
                        format=' %(levelname)s - %(asctime)s - %(message)s',
                        level=logging.INFO)
    # Target URL
    target_url = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5000/VNP46A2/2020/005/VNP46A2.A2020005.h09v05.001.2021053061355.h5"
    # Connect to LAADS
    s = connect_to_laads()
    # Make the request and get the content
    get_VIIRS_file(s, target_url)


if __name__ == '__main__':

    main()