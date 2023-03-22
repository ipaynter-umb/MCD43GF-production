import datetime
import t_spinup
import c_laads
import t_misc
import logging
import hashlib
from os import environ, walk, remove
from numpy import arange
from pathlib import Path


# Main function
def main():
    # Set the logging config
    logging.basicConfig(filename=environ['logs_dir'] + f'/{datetime.datetime.now():%Y%m%d%H%M%S}.log',
                        filemode='w',
                        format=' %(levelname)s - %(asctime)s - %(message)s',
                        level=logging.INFO)
    # Band list
    band_list = [31, 40]
    # List of bands from 1 - 31
    band_list += list(arange(1, 31, 1))
    # For band in bands
    for band in band_list:
        # Log info
        logging.info(f"Getting catalog for band {band}.")
        # Get a LAADS data set object (this will generate the catalog)
        dataset = c_laads.LAADSDataSet(f'MCD43D{t_misc.zero_pad_number(band, digits=2)}',
                                       archive_set='61',
                                       product=f'MCD43D{t_misc.zero_pad_number(band, digits=2)}')
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
                    file_hash = hashlib.md5(
                        open(Path(environ["inputs_dir"], dataset.name, name), 'rb').read()).hexdigest()
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


if __name__ == "__main__":

    main()
