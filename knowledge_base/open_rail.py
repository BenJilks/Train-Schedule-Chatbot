import requests
import json
import time
import sys
import urllib.parse
import os
import shutil
from zipfile import ZipFile

# FIXME: This should probably be in a config file
DTD_DATABASE_PATH = 'dtd_data'
DTD_EXPIRY = 60 * 60 * 24 # 1 day
CREDENTIALS = ('benjyjilks@gmail.com', '2n3gfJUdxGizAHF%')
    
def is_dtd_outdated():
    if not os.path.exists(DTD_DATABASE_PATH):
        return True

    files = os.listdir(DTD_DATABASE_PATH)
    if len(files) == 0:
        return True

    last_modified = os.path.getmtime(DTD_DATABASE_PATH + '/' + files[0])
    age = time.time() - last_modified
    if age >= DTD_EXPIRY:
        return True

    return False

def generate_dtd_token() -> str:
    AUTHENTICATE_URL = 'https://opendata.nationalrail.co.uk/authenticate'
    HEADERS = { 'Content-Type': 'application/x-www-form-urlencoded' }

    response = requests.post(AUTHENTICATE_URL, headers=HEADERS, 
        data=f"username={ CREDENTIALS[0] }&password={ urllib.parse.quote_plus(CREDENTIALS[1]) }")

    response_json = json.loads(response.text)
    return response_json['token']

def download_dtd_zip_file(token: str) -> str:
    FARES_URL = 'https://opendata.nationalrail.co.uk/api/staticfeeds/2.0/fares'
    HEADERS = { 
        'Content-Type': 'application/json',
        'X-Auth-Token': token,
    }
    response = requests.get(FARES_URL, headers=HEADERS, stream=True)

    disposition = response.headers['Content-Disposition']
    length = int(response.headers['Content-Length'])
    filename = disposition.split(';')[-1].split('=')[-1].strip()[1:-1]

    print(f"Downloading file '{ filename }' ({ length } bytes)")
    downloaded = 0
    progress = 0
    with open(DTD_DATABASE_PATH + '/' + filename, 'wb') as f:
        for chunk in response.iter_content(chunk_size=128):
            f.write(chunk)
            downloaded += 128
            progress += 128
            if progress / length >= 0.1:
                print('.', end='')
                sys.stdout.flush()
                progress = 0
    print()

    return filename

def update_dtd_database():
    if not is_dtd_outdated():
        return

    print('Updating DTD database')
    shutil.rmtree(DTD_DATABASE_PATH)
    os.makedirs(DTD_DATABASE_PATH, exist_ok=True)
    token = generate_dtd_token()
    fares_zip = download_dtd_zip_file(token)

    print(f"Extracting '{ fares_zip }'")
    with ZipFile(DTD_DATABASE_PATH + '/' + fares_zip, 'r') as f:
        f.extractall(DTD_DATABASE_PATH)
    os.remove(DTD_DATABASE_PATH + '/' + fares_zip)
    print('Finished')

def find_dtd_file(extention: str) -> str:
    filename = None
    for file in os.listdir(DTD_DATABASE_PATH):
        if file.endswith(extention):
            filename = file
            break

    if filename == None:
        raise Exception(f'No { extention } file in DTD database')

    return filename

def find_ncl_code_from_crs(crs: str) -> str | None:
    loc_filename = find_dtd_file('LOC')
    with open(DTD_DATABASE_PATH + '/' + loc_filename, 'r') as loc:
        for line in loc:
            entry_type = line[:2]
            if entry_type != 'RL':
                continue

            # Location record
            nlc_code = line[36:40]
            crs_code = line[56:59]
            if crs_code == crs:
                return nlc_code

    return None

def fares_for(from_location: str, to_location: str) -> dict[str, list[int]]:
    update_dtd_database()
    fares = {}

    from_ncl = find_ncl_code_from_crs(from_location)
    to_ncl = find_ncl_code_from_crs(to_location)
    if from_ncl == None or to_ncl == None:
        return {}

    ffl_filename = find_dtd_file('FFL')
    with open(DTD_DATABASE_PATH + '/' + ffl_filename, 'r') as ffl:
        for line in ffl:
            entry_type = line[:2]
            if entry_type == 'RF':
                # Flow record
                origin_code = line[2:6]
                destination_code = line[6:10]
                direction = line[19]
                if not ((origin_code == from_ncl and destination_code == to_ncl) or 
                    (direction == 'R' and origin_code == to_ncl and destination_code == from_ncl)):
                    continue

                flow_id = line[42:49]
                fares[flow_id] = []
            elif entry_type == 'RT':
                # Fare record
                flow_id = line[2:9]
                if not flow_id in fares.keys():
                    continue

                fare = int(line[12:20])
                fares[flow_id].append(fare)
    return fares

