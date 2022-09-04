import t_mcd43gf
import t_laads_tools
from datetime import date, timedelta
import datetime
from pathlib import Path
import logging
from os import environ
from dotenv import load_dotenv

load_dotenv()

# Set the logging config
logging.basicConfig(filename=environ['log_files_path'] + f'{datetime.datetime.now():%Y%m%d%H%M%S}.log',
                    filemode='w',
                    format=' %(levelname)s - %(asctime)s - %(message)s',
                    level=logging.INFO)

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

t_laads_tools.multithread_download(target_list, workers=3)