import argparse
import t_mcd43gf
import logging
from os import environ

# Set the logging config
logging.basicConfig(filename=environ['log_files_path'] + f'{datetime.datetime.now():%Y%m%d%H%M%S}.log',
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
    logging.info(f"Processing band {band}.")
    # Get the input data for the band
    get_input_data_for_band(years, band, archive_set=archive_set)

if __name__ == '__main__':

    # Make an argument parser instance
    parser = argparse.ArgumentParser()
    # Add an argument for the years
    parser.add_argument('-y',
                        '--years',
                        nargs='+',
                        help='Enter years (space-separated)',
                        required=True)
    # Parse any argument
    args = parser.parse_args()
    # Get input data for years
    t_mcd43gf.get_input_data_for_gapfilled(args.years, archive_set=61)
    # Create the symbolic links
    t_mcd43gf.create_symbolic_links(args.years, archive_set=61)
