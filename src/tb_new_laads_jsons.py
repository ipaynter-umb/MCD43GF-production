import tb_laads_tools
from dotenv import load_dotenv
from os import environ
from time import time
import numpy as np
import datetime
from os import walk, environ
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

ds = tb_laads_tools.LAADsDataSet('first_test',
                                 archive_set='5000',
                                 product='VNP46A2',
                                 start_date=datetime.date(year=2020, month=2, day=10),
                                 end_date=datetime.date(year=2021, month=3, day=30))

exit()


def unpack_dict(dictionary, unpacked_list=None, current_keys=None):

    # If no ongoing unpacked list provided
    if not unpacked_list:
        # Make empty list
        unpacked_list = []
    # If no ongoing current keys provided
    if not current_keys:
        # Make empty list
        current_keys = []
    # Retrieve a list of dictionary items
    list_of_items = list(dictionary.items())
    # While there are items remaining in the list
    while list_of_items:
        # Get an item (key: value)
        item_from_list = list_of_items.pop()
        # If the key points to a value that is a dictionary
        if isinstance(item_from_list[1], dict):
            # Unpack it (send it to this function using the current unpacked list and keys)
            unpacked_list = unpack_dict(item_from_list[1],
                                        unpacked_list=unpacked_list,
                                        current_keys=current_keys + [item_from_list[0]])
        # Otherwise (value is not a dictionary)
        else:
            # Add a tuple of the list of keys and the value to the unpacked list
            unpacked_list.append((current_keys + [item_from_list[0]], item_from_list[1]))
    # Return the list of tuples where each is (key list, value)
    return unpacked_list


def unpack_request(r):
    # Output dictionary
    output_dict = {'items': []}
    # Convert to dictionary
    rdict = r.json()
    # For each piece of content
    for content in rdict['content']:
        # If it's a Directory
        if content['resourceType'] == 'Directory':
            # Add to dictionary
            output_dict[content['self']] = {}
        # Otherwise (not a Directory)
        else:
            # Add to list
            output_dict['files'].append(content['self'])
    return output_dict

# Load environment variables
load_dotenv()
# Connect an HTTPS session to LAADS
session = tb_laads_tools.connect_to_laads()
# Base URL
target_url = 'https://ladsweb.modaps.eosdis.nasa.gov'
# Start timing
stime = time()
# Make request to allData json
r = tb_laads_tools.attempt_request(session, target_url + '/archive/allData.json')

print(f'Request in {np.around(time() - stime, decimals=2)} seconds.')
# Convert to dictionary
archive_sets = r.json()

for archive_set in archive_sets['content']:
    archive_set_name = archive_set['name']
    try:
        int(archive_set_name)
    except:
        print(f'Archive set {archive_set_name} does not conform to integer format')
    # If
    if archive_set['resourceType'] == 'Directory':
        #print(archive_set['self'])
        r = tb_laads_tools.attempt_request(session, target_url + archive_set['self'] + '.json')
        products = r.json()
        for product in products['content']:
            product_cksum_checked = False
            product_name = product['name']
            # If
            if product['resourceType'] == 'Directory':
                print(product['self'])
                r = tb_laads_tools.attempt_request(session, target_url + product['self'] + '.json')
                years = r.json()
                for year in years['content']:
                    year_name = year['name']
                    try:
                        curr_date = datetime.date(year=(int(year_name)),
                                                  day=1,
                                                  month=1)
                    except:
                        print(f'Year {year_name} in product {product_name} in archive set {archive_set_name} is not a valid year.')
                    # If
                    if year['resourceType'] == 'Directory':
                        #print(year['self'])
                        r = tb_laads_tools.attempt_request(session, target_url + year['self'] + '.json')
                        doys = r.json()
                        for doy in doys['content']:
                            doy_name = doy['name']
                            try:
                                curr_date = datetime.date(year=(int(year_name)),
                                                          day=1,
                                                          month=1) + datetime.timedelta(days=int(doy_name) - 1)
                            except:
                                print(f'Day of Year {doy_name} in year {year_name} in product {product_name} in archive set {archive_set_name} is not a valid day of year.')
                                # If
                            if doy['resourceType'] == 'Directory':
                                #print(doy['self'])
                                r = tb_laads_tools.attempt_request(session, target_url + doy['self'] + '.json', attempt_limit=1)

                                # If we got the file (will be False otherwise)
                                if r:
                                    files = r.json()
                                    for file in files['content']:
                                        file_name = file['name']
                                        if file['resourceType'] == 'Directory':
                                            print(
                                                f'File {file_name} from DOY {doy_name} in year {year_name} in product {product_name} in archive set {archive_set_name} has a subdirectory.')
                                        if 'cksum' not in file.keys():
                                            print(
                                                f'File {file_name} from DOY {doy_name} in year {year_name} in product {product_name} in archive set {archive_set_name} does not have a checksum.')
                                        product_cksum_checked = True
                                        break
                                else:
                                    break
                            if product_cksum_checked:
                                break
                    if product_cksum_checked:
                        break
