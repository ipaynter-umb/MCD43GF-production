from pathlib import Path
import c_laads

band_list = list(range(1, 22, 1))

for band in band_list:
    print(round((band + 1) / 3))


print("MCD43D04"[-2:])

Path((1, 2))