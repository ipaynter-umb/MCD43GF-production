import t_laads_tools
import logging
import datetime
from os import environ
import dotenv

# Load environmental variables
dotenv.load_dotenv()

# Set the logging config
logging.basicConfig(filename=environ['log_files_path'] + f'{datetime.datetime.now():%Y%m%d%H%M%S}.log',
                    filemode='w',
                    format=' %(levelname)s - %(asctime)s - %(message)s',
                    level=logging.NOTSET)

# Get a list of product suffixes from 1 - 31
product_suffixes = range(1, 32, 1)

# List to hold product lists
product_list = []

# For each suffix in the list
for suffix in product_suffixes:
    # Construct the product full name (zero-padding the suffix to 2 digits) and append to list
    product_list.append(f'MCD43D{t_laads_tools.zero_pad_number(suffix, digits=2)}')

# Append the last product to the list
product_list.append('MCD43D40')

#for index, product in enumerate(product_list):
#    print(index + 1, product)

# Send the product list to the multithreading
t_laads_tools.multithread_spidering(product_list,
                                    archive_set=6,
                                    workers=5)
