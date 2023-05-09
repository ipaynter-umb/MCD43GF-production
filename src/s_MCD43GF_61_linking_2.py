import c_laads
import t_mcd43gf
import t_spinup
import t_misc
import t_laads
import datetime
import logging
from os import environ, mkdir, symlink
from os.path import exists
from pathlib import Path
from numpy import arange


# Takes a LAADSDataSet object and a destination directory
def create_symbolic_links(year, band, datasets):

    year_dir = Path(environ['inputs_dir'], 'links', str(year))

    if not exists(year_dir):
        mkdir(year_dir)

    band_dir = Path(year_dir, band)

    if not exists(band_dir):
        mkdir(band_dir)

    start_date = datetime.date(year=year - 1, month=6, day=20)
    end_date = datetime.date(year=year + 1, month=1, day=1) + datetime.timedelta(days=192)

    current_date = start_date

    dest_dir = Path(environ['inputs_dir'], 'links', str(year), band)

    while current_date < end_date:
        for dataset in datasets:
            if current_date in dataset.by_date.keys():
                # For each file in the date
                for file in dataset.by_date[current_date]:
                    # Current file name
                    filename = file.name
                    # Path to the file
                    file_path = Path(environ['inputs_dir'], dataset.name, filename)
                    # Subyear path
                    subyear_dir = Path(dest_dir, str(current_date.year))
                    if not exists(subyear_dir):
                        mkdir(subyear_dir)
                    # Get the link path
                    link_path = Path(subyear_dir, filename)
                    # If the link does not already exist
                    if not exists(link_path):
                        # Create a symbolic link
                        symlink(file_path, link_path)
                break
        current_date = current_date + datetime.timedelta(days=1)


# Main function
def main():
    # Set the logging config
    logging.basicConfig(filename=environ['logs_dir'] + f'/{datetime.datetime.now():%Y%m%d%H%M%S}.log',
                        filemode='w',
                        format=' %(levelname)s - %(asctime)s - %(message)s',
                        level=logging.INFO)

    for year in [2017, 2018]:
        # Band list
        band_list = [31, 40]
        # List of bands from 1 - 31
        band_list += list(arange(1, 22, 1))
        # For band in bands
        for band in band_list:
            datasets = [c_laads.LAADSDataSet(f'MCD43D{t_misc.zero_pad_number(band, digits=2)}_1'),
                        c_laads.LAADSDataSet(f'MCD43D{t_misc.zero_pad_number(band, digits=2)}_2')]
            # Create the symlinks
            create_symbolic_links(year, t_misc.zero_pad_number(band), datasets)


if __name__ == "__main__":

    main()
