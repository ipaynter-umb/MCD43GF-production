import t_mcd43gf
import t_laads_tools
from datetime import date, timedelta
import datetime
from pathlib import Path
import logging
from os import environ
from dotenv import load_dotenv
from time import time
from numpy import around
from concurrent.futures import ProcessPoolExecutor, as_completed

load_dotenv()

if __name__ == '__main__':

    # Set the logging config
    logging.basicConfig(filename=environ['log_files_path'] + f'{datetime.datetime.now():%Y%m%d%H%M%S}.log',
                        filemode='w',
                        format=' %(levelname)s - %(asctime)s - %(message)s',
                        level=logging.INFO)

    #ed_dict = t_laads_tools.EarthDataDict("MCD43D31", archive_set='6')

    #file_path = Path(r"F:\UMB\MCD43GF\input\6\MCD43D31.A2001065.006.2016113141635.hdf")

    #stime = time()

    #checksum_test = t_laads_tools.check_checksum(file_path, ed_dict.by_name['MCD43D31.A2001065.006.2016113141635.hdf']['checksum'])

    #print(f'Checksum matched: {checksum_test} in {around(time() - stime, decimals=2)} seconds.')

    ed_dict = t_laads_tools.EarthDataDict("MCD43D31", archive_set='6')

    start_date = date(year=2001, month=3, day=1)
    end_date = date(year=2001, month=3, day=10)

    curr_date = start_date

    target_list = []

    while curr_date <= end_date:
        file_object = ed_dict.get_file_objects_from_date(curr_date)
        file_object = file_object[0]
        file_object.destination = Path('F:/UMB/MCD43GF/input', '6', file_object.name)
        if file_object:
            target_list.append(file_object)

        curr_date += timedelta(days=1)

    print(target_list)
    dl_list = []
    # Start a ProcessPoolExecutor
    with ProcessPoolExecutor(max_workers=2) as executor:
        # Submit the tasks from the target list to the worker function
        future_events = {executor.submit(t_mcd43gf.multithread_check_checksum,
                                         target): target for target in target_list}
        # As each worker is finished
        for event in as_completed(future_events):
            #print(event)
            # Get the results dictionary
            checksum_result = event.result()
            # If the checksum check failed
            if checksum_result is False:
                # Log this occurrence
                logging.warning(f"Checksum did not match validation for {future_events[event].name}. Attempting to redownload.")
                # Add the target to the download list
                dl_list.append(future_events[event])
        # Add a day to the current date
        curr_date += datetime.timedelta(days=1)
    print(dl_list)
    #t_laads_tools.multithread_download(target_list, workers=3)