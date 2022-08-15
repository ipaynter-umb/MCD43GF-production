import requests
import h5py
import io

# fake_token = "aWFuc2104kshlcjE6YVdGdWNHRjViblJsY2pGQVoyTWL106d1WTI5dDoxNjYwMzMxMTc3OmNkODk5YTExNmExZmUyOTM5MDMxMGM4MmNhZmE5MDc3YjEzPEW03MF"
#fake_token = "aWFuc"

fake_token = "badtoken"

# Connect to LAADS and return a session object
def connect_to_laads():
    # Header command utilizing security token
    authToken = {'Authorization': f'Bearer {fake_token}'}
    # Create session
    s = requests.session()
    # Update header with authorization
    s.headers.update(authToken)
    # Return the session object
    return s


# Get a VIIRS H5 file from laads and return it in some form
def get_VIIRS_file(session_obj, target_url, write_local=False, return_content=False, return_file=True):
    # Request the H5 file from the provided URL
    r = session_obj.get(target_url)

    print(r.status_code)
    print(len(r.content))
    with open("F:/UMB/testfile.hdf", 'wb') as f:
        # <> Compare against the checksum here (LAADS provides CRC32)
        # Initiate CRC counter: crc = 0
        # For each line: for line in f:
        # Update the CRC count: crc = binascii.crc32(line, crc)
        f.write(r.content)

    return r.content


if __name__ == '__main__':

    target_url = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/6/MCD43D01/2002/005/MCD43D01.A2002005.006.2016125182031.hdf"

    s = connect_to_laads()

    hfile = get_VIIRS_file(s, target_url)

    #print(io.BytesIO(r.content))