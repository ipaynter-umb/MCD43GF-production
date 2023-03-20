import tb_laads_tools
import h5py
import io
import numpy as np

s = tb_laads_tools.get_laads_session()

years = s.get("https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5000/VNP46A2.json")

for year in years.json()['content']:
    doys = s.get(f"https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5000/VNP46A2/{year['name']}.json")
    for doy in doys.json()['content']:
        files = s.get(f"https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5000/VNP46A2/{year['name']}/{doy['name']}.json")
        print(files.json()['content'][0]['downloadsLink'])
        input()

for doy in np.range(0, 365):
    url = f"https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5000/VNP46A2/2020/{doy}/VNP46A2.A2020040.h00v06.001.2021053191602.h5"

url = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5000/VNP46A2/2020/040/VNP46A2.A2020040.h00v06.001.2021053191602.h5"

r = s.get(url, allow_redirects=False)

print(r.status_code)

# Convert the response content to an H5py File object and return
h5file = h5py.File(io.BytesIO(r.content), 'r')

print(h5file.keys())

print(h5file['HDFEOS'].keys())

for h5_key in h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields'].keys():
    print(h5_key)

ntl_qf = np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['Mandatory_Quality_Flag'])

ntl_array = np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['DNB_BRDF-Corrected_NTL'])

print(type(ntl_array))

print(ntl_array.shape)

print(f'Out of {ntl_array.size} pixels, {np.count_nonzero(ntl_array == 65535)} are fill values')
print(f'Out of {ntl_qf.size} pixels, {np.count_nonzero(ntl_qf == 255)} are fill values,\n'
      f'{np.count_nonzero(ntl_qf == 1)} are high quality values (consistent lights)\n'
      f'{np.count_nonzero(ntl_qf == 2)} are high quality values (ephemeral lights)\n'
      f'{np.count_nonzero(ntl_qf == 3)} are poor quality values (ephemeral lights)')