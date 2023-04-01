import hashlib
from pathlib import Path
import c_laads
import t_laads
import datetime
import logging
from os import environ

# Set the logging config
logging.basicConfig(filename=environ['logs_dir'] + f'/{datetime.datetime.now():%Y%m%d%H%M%S}.log',
                    filemode='w',
                    format=' %(levelname)s - %(asctime)s - %(message)s',
                    level=logging.INFO)

# Get a LAADS data set object (this will generate the catalog)
dataset = c_laads.LAADSDataSet(f'HashInvestigation',
                               archive_set='61',
                               product=f'MCD43D31',
                               start_date=datetime.date(year=2000, month=4, day=5),
                               end_date=datetime.date(year=2000, month=4, day=9))

dataset.download_catalog()

exit()

file_name = "MCD43D31.A2000096.061.2020042005857.hdf"

file_path = Path(f"F:/UMB/MCD43GF/inputs/MCD43D31/{file_name}")

file_hash = hashlib.md5(
                        open(file_path, 'rb').read()).hexdigest()

print(file_hash)

dataset = c_laads.LAADSDataSet("MCD43D31")

print(dataset.by_filename[file_name].hash)

response = t_laads.get_laads_hdf4(('https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MCD43D31/2000/096/MCD43D31.A2000096.061.2020042005857.hdf',
                        file_hash))

print(hashlib.md5(response[1].content).hexdigest())
