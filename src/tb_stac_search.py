import json
import requests

s = requests.session()

#r = s.get("https://cmr.earthdata.nasa.gov/stac")

#in_dict = json.loads(r.text)

#for subdict in in_dict['links']:
#    print(subdict['title'], subdict['href'])

root = "https://cmr.earthdata.nasa.gov/stac/LPCLOUD/collections/"
s30 = root + "HLSS30.v2.0"
l30 = root + "HLSL30.v2.0"
ast = root + "ASTGTM.v003"

for dataset in [s30, l30]:
    dataset_dict = json.loads(s.get(dataset).text)
    # For each year (dictionary in list in links)
    for year_link in dataset_dict['links']:
        if year_link['rel'] == 'child':
            print(year_link['title'], year_link['href'])
            # Get the year
            year_dict = json.loads(s.get(year_link['href']).text)
            # For each month (dictionary in list in links)
            for month_link in year_dict['links']:
                if month_link['rel'] == 'child':
                    print(f"    {month_link['title']}, {month_link['href']}")
                    # Get the month
                    month_dict = json.loads(s.get(month_link['href']).text)
                    # For each day (dictionary in list in links)
                    for day_link in month_dict['links']:
                        if day_link['rel'] == 'child':
                            print(f"        {day_link['title']}, {day_link['href']}")
                            # Get the day
                            day_dict = json.loads(s.get(day_link['href']).text)
                            # For each item (dictionary in list in links)
                            for item_link in day_dict['links']:
                                if item_link['rel'] == 'item':
                                    print(f"            {item_link['title']}, {item_link['href']}")
                                    # Get the item
                                    item_dict = json.loads(s.get(item_link['href']).text)
                                    # For each asset (dictionary in list in links)
                                    for asset_key in item_dict['assets'].keys():
                                        print(f"                {asset_key}, {item_dict['assets'][asset_key]['href'].split('/')[-1]}")
                                    input()