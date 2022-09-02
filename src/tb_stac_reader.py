import json
from pathlib import Path

file_path = Path("F:/UMB/LPHLSStac.json")

with open(file_path, 'r') as f:
    in_dict = json.load(f)

stac_version = in_dict['stac_version']
number_matched = in_dict['numberMatched']
number_returned = in_dict['numberReturned']

print(f'STAC Version: {stac_version}')
print(f'Number of files matched: {number_matched}')
print(f'Number of files returned: {number_returned}')

for subdict in in_dict['features']:
    print(subdict['id'])

    #for key in subdict:
    #    print(key, subdict[key])

exit()


for key in in_dict.keys():
    print(key)
    try:
        for subkey in in_dict[key].keys():
            print(f'    {subkey}')
    except:

        try:
            print(f'    {in_dict.get(key)}')
        except:
            print(f'{key} has no item.')

        # try:
        #
        #     for iterable in key:
        #         print(f'    {iterable}')
        # except:
        #     print(f'{key} is not iterable')
        print(f'{key} has no subdict')
    #print(key, in_dict.get(key))
