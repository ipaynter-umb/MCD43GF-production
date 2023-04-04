import datetime
import t_spinup
import c_laads
import t_misc
import logging
from os import environ
from numpy import arange
import t_mcd43gf
from pathlib import Path


# Main function
def main():
    # Set the logging config
    logging.basicConfig(filename=environ['logs_dir'] + f'/{datetime.datetime.now():%Y%m%d%H%M%S}.log',
                        filemode='w',
                        format=' %(levelname)s - %(asctime)s - %(message)s',
                        level=logging.INFO)
    # Output dir
    output_dir = Path(environ['inputs_dir'], 'links')

    # Band list
    band_list = [31, 40]
    # List of bands from 1 - 31
    band_list += list(arange(1, 31, 1))
    # For band in bands
    for band in band_list:
        # Log info
        logging.info(f"Getting catalog for band {band}.")
        # Get a LAADS data set object (this will generate the catalog)
        dataset = c_laads.LAADSDataSet(f'owl_MCD43D{t_misc.zero_pad_number(band, digits=2)}')
        # Download dataset full catalog
        t_mcd43gf.create_symbolic_links(dataset, output_dir)


if __name__ == "__main__":

    main()
