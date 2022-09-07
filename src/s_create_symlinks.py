import t_mcd43gf
import logging
import datetime
from os import environ
from dotenv import load_dotenv

load_dotenv()


def main():

    # Set the logging config
    logging.basicConfig(filename=environ['log_files_path'] + f'{datetime.datetime.now():%Y%m%d%H%M%S}.log',
                        filemode='w',
                        format=' %(levelname)s - %(asctime)s - %(message)s',
                        level=logging.INFO)

    year_list = ['2018', '2019', '2020', '2021', '2022']

    archive_set = '6'

    t_mcd43gf.create_symbolic_links(year_list, archive_set=archive_set)


if __name__ == '__main__':

    main()