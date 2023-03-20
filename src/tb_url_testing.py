from pathlib import Path
import t_requests
import t_laads

target = str("https:\/\/ladsweb.modaps.eosdis.nasa.gov\/archive\/allData\/62").replace('\\', '')
target += '.json'
print(target)
doy_json = t_requests.ask_nicely(t_laads.get_laads_session(),
                                 target,
                                 validation_func=t_requests.validate_request_json,
                                 )