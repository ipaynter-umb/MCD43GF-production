import datetime
import t_spinup
import c_laads
import t_misc
import logging
import hashlib
import datetime
from os import environ, walk, remove
from numpy import arange
from pathlib import Path
from io import BytesIO


# Main function
def main():
    # Set the logging config
    logging.basicConfig(filename=environ['logs_dir'] + f'/{datetime.datetime.now():%Y%m%d%H%M%S}.log',
                        filemode='w',
                        format=' %(levelname)s - %(asctime)s - %(message)s',
                        level=logging.INFO)

    # Get a LAADS data set object (this will generate the catalog)
    dataset = c_laads.LAADSDataSet(f'VNPBootstrap',
                                   archive_set='5000',
                                   product=f'VNP46A2',
                                   start_date=datetime.date(year=2020, month=9, day=22),
                                   end_date=datetime.date(year=2020, month=9, day=22),
                                   include='h10')
    # Get datetime string
    now_str = datetime.datetime.now().strftime("%m%d%Y_%H%M%S")

    # Open a download log in write mode
    of = open(Path(environ['support_dir'],
                   f'{dataset.name}_download_{now_str}.txt'),
              mode='w')

    # Walk the download directory
    for root, dirs, files in walk(Path(environ["inputs_dir"], dataset.name)):

        # For each file name
        for name in files:
            # If the name is not in the catalog
            if name not in dataset.by_filename.keys():
                # Delete the file
                remove(Path(environ["inputs_dir"], dataset.name, name))
            # Otherwise (file is in catalog)
            else:
                file_hash = hashlib.md5(open(Path(environ["inputs_dir"], dataset.name, name), 'rb').read()).hexdigest()
                # If the hash matches the reference
                if file_hash == dataset.by_filename[name].hash:
                    # Write a line to the download log
                    of.write(f'{name} True\n')
                # Otherwise (hash does not match)
                else:
                    # Delete the file
                    remove(Path(environ["inputs_dir"], dataset.name, name))
    # Close the download record file
    of.close()
    # Download the catalog
    dataset.download_catalog()


if __name__ == "__main__":

    main()
