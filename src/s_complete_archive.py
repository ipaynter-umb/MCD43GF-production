import json
import t_laads_tools
import logging
import datetime
from pathlib import Path
from os import environ
from dotenv import load_dotenv

load_dotenv()

def main():

    # Set the logging config
    logging.basicConfig(filename=environ['log_files_path'] + f'{datetime.datetime.now():%Y%m%d%H%M%S}.log',
                        filemode='w',
                        format=' %(levelname)s - %(asctime)s - %(message)s',
                        level=logging.INFO)

    file_path = Path(environ['log_files_path'], '20220907105725.log')
    file_log = open(file_path, 'r')

    link_fails = 0
    failed_links = {}

    while True:

        line = file_log.readline()

        if not line:
            break

        # Split the line
        split_line = line.split('-')

        # Get the level
        level = split_line[0].strip()

        # If this is warning
        if level == "WARNING":
            if "Could not create link" in split_line[4]:


                file_name = split_line[4].split(' ')[6].strip()[:-1]

                # Get the product name
                product = file_name.split('.')[0]
                if product not in failed_links.keys():
                    failed_links[product] = []
                if file_name not in failed_links[product]:
                    link_fails += 1
                    failed_links[product].append(file_name)

    logging.info(f'Total of {link_fails} files are missing from the archive.')

    targets = []

    for product in failed_links.keys():
        # Get the Earth Data Dictionary for the product
        product_dict = t_laads_tools.EarthDataDict(product, archive_set='6')
        # For each link (file name)
        for link in failed_links[product]:
            # Add a file request to the targets list
            targets.append(t_laads_tools.EarthDataFileRequest(link,
                                                              product_dict.by_name[link]['url'],
                                                              Path(environ['input_files_path'], product_dict.archive_set, link),
                                                              checksum=product_dict.by_name[link]['checksum']))

    # Send the targets to the download function
    t_laads_tools.multithread_download(targets)

if __name__ == '__main__':

    main()