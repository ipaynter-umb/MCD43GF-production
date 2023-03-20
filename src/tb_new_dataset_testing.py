import tb_laads_tools
from datetime import date, datetime
import dotenv
import logging
from os import environ

# Load environmental variables
dotenv.load_dotenv()

def main():

    # Set the logging config
    logging.basicConfig(filename=environ['log_files_path'] + f'{datetime.now():%Y%m%d%H%M%S}.log',
                        filemode='w',
                        format=' %(levelname)s - %(asctime)s - %(message)s',
                        level=logging.DEBUG)

    dataset = tb_laads_tools.LAADsDataSet('Test001')

    dataset.download_catalog()

if __name__ == '__main__':

    main()

# dataset = tb_laads_tools.LAADsDataSet('Test001',
#                                       archive_set='61',
#                                       product="MCD43D25",
#                                       start_date=date(year=2021, month=1, day=1),
#                                       end_date=date(year=2021, month=3, day=1))
#
# dataset.download_catalog()