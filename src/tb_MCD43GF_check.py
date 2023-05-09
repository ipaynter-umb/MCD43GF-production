import t_spinup
from pathlib import Path
from os.path import exists
import rioxarray as rxr

file_path = Path("F:/UMB/Test_MCD43GF/MCD43GF_vol_Band2_309_2002_V006.hdf")

modis_pre = rxr.open_rasterio(file_path, masked=True)

