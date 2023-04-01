import datetime
import t_spinup
import c_laads
import t_misc
import logging
from os import environ
from numpy import arange


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
        dataset = c_laads.LAADSDataSet(f'toki_MCD43D{t_misc.zero_pad_number(band, digits=2)}',
                                       archive_set='61',
                                       product=f'MCD43D{t_misc.zero_pad_number(band, digits=2)}',
                                       start_date=datetime.date(year=2020, month=1, day=5),
                                       end_date=datetime.date(year=2020, month=1, day=5))
        # Download dataset full catalog
        dataset.download_catalog()


if __name__ == "__main__":

    main()
