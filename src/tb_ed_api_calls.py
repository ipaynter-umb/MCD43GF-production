from pathlib import Path
import requests
import datetime
from os import environ
from dotenv import load_dotenv
import t_laads_tools
import lxml.html as lh

load_dotenv()

base_url = "https://ladsweb.modaps.eosdis.nasa.gov"

def get_date_range(product, archive_set='5000'):

    # Get an authenticated session
    s = t_laads_tools.connect_to_laads()
    # Base URL for product/archive set
    url = environ['laads_alldata_url'] + archive_set + '/' + product
    # Request the json for the product/archive set
    r = s.get(url + '.json')

    year_list = []

    for year_dict in r.json():
        year_list.append(year_dict['name'])
    sorted_years = sorted(year_list, key=int)

    # Get the days for the earliest year
    r = s.get(url + f'/{sorted_years[0]}.json')

    doy_list = []

    for doy_dict in r.json():
        doy_list.append(doy_dict['name'])

    sorted_doys = sorted(doy_list, key=int)
    earliest_doy = sorted_doys[0]

    # Get the days for the latest year
    r = s.get(url + f'/{sorted_years[-1]}.json')

    doy_list = []

    for doy_dict in r.json():
        doy_list.append(doy_dict['name'])

    sorted_doys = sorted(doy_list, key=int)
    latest_doy = sorted_doys[-1]

    start_date = datetime.date(year=int(sorted_years[0]),
                               month=1,
                               day=1) + datetime.timedelta(days=int(earliest_doy))

    end_date = datetime.date(year=int(sorted_years[-1]),
                             month=1,
                             day=1) + datetime.timedelta(days=int(latest_doy))

    return start_date, end_date





def last_day_of_month(start_date):
    last_day = start_date
    while start_date.month == last_day.month:
        last_day = last_day + datetime.timedelta(days=1)
    last_day = last_day - datetime.timedelta(days=1)
#     print(last_day_of_month.isoformat())
    return last_day

product = "MCD43D01"
collection = "6"

# Chunk size (number of days to search at a time)
chunk_size = 50

# Get the start and end dates for the product / archive set combination
product_start, product_end = get_date_range(product, archive_set=collection)

# Set current date tracker to start date for product
current_date = product_start

# Make a new, un-authenticated session
s = requests.Session()

# While the current date is less than the end date
while current_date < product_end:
    # Current end date (based on current date and chunk size)
    current_end = current_date + datetime.timedelta(days=chunk_size)
    # Assemble a search based on the current date and chunk size
    product_search = f"/api/v1/files/product={product}&collection={collection}&dateRanges={current_date.isoformat()}..{current_end.isoformat()}"
    # Make a request based on the search
    r = s.get(base_url + product_search, allow_redirects=False)
    print(r.status_code)
    #print(r.text)
    #print(r.json())
    print(f'Status code {r.status_code} for search {product_search}')
    #print(f'Number of files returned {len(r.json())}')
    print('Files IDs and names')
    for file_id in r.json():
        checksum_url = f'https://ladsweb.modaps.eosdis.nasa.gov/api/v1/filePage/useSimple=true&fileId={file_id}'

        # Get the target url
        checksum_request = s.get(checksum_url, allow_redirects=False)
        # Retrieve the text
        html = checksum_request.text
        # Switch for when the checksum will be in the next td
        checksum_next = False
        # Checksum placeholder
        checksum = None
        # Get the root
        root = lh.fromstring(html)
        # For each element in the root
        for element in root.iter():
            # If it's a table
            if element.attrib.get('class') == "table":
                # For each row in the table
                for tr in element:
                    # For each division in the row
                    for td in tr:
                        # For the text in the division
                        for text in td.itertext():
                            # If this is the checksum
                            if checksum_next:
                                # Try converting checksum to integer
                                try:
                                    int(text)
                                    # Return it
                                    checksum = text
                                except:
                                    pass
                                    #logging.error(f"Checksum {text} is not a valid checksum.")
                            # If the text reads "Checksum"
                            if text == "Checksum":
                                # Flip the checksum next switch
                                checksum_next = True

        #checksum = t_laads_tools.get_checksum(checksum_url)

        print(f"{file_id}: {r.json()[file_id]['name']}, checksum: {checksum}")
    # Add to current date
    current_date += datetime.timedelta(days=chunk_size)
    exit()

#print(r.json())

#for product in r.json():
#    print(product['ESDT'])
#    if product['ESDT'] == 'VNP46A2':
#        for key in product.keys():
#            print('  ', key, product[key])


#for file_id in r.json():
#    print(f"{file_id}: {r.json()[file_id]['name']}")

#print("\n".join([x for x in r.json()]))