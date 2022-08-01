import t_laads_tools
import numpy as np

t_laads_tools.multithread_spidering(["MCD43D01",
                                     "MCD43D02",
                                     "MCD43D03"],
                                    archive_set=6)

exit()

url_dict = t_laads_tools.LaadsUrlsDict("MCD43D01", archive_set=6)

for location in url_dict.dictionary.keys():
    for year in url_dict.dictionary[location].keys():
        for doy in url_dict.dictionary[location][year].keys():
            print(f"{location}, {year}, {doy}: {url_dict.dictionary[location][year][doy]}")
