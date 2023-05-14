import datetime
import t_spinup
import c_laads
import t_misc
import logging
import t_mcd43gf
from os import environ
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
    band_list += list(arange(1, 22, 1))
    # For band in bands
    for band in band_list:
        # Log info
        logging.info(f"Getting catalog for band {band}.")
        # Get a LAADS data set object (this will generate the catalog)
        dataset = c_laads.LAADSDataSet(f'MCD43D{t_misc.zero_pad_number(band, digits=2)}_4')
        # Print the download report
        dataset.print_download_report()


if __name__ == "__main__":

    main()
