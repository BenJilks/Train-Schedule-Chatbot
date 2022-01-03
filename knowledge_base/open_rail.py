import requests
import json
import time
import sys
import urllib.parse
import os
import tempfile
import sqlalchemy
import shutil
from sqlalchemy.engine.base import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import Session, sessionmaker
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import Integer, String
from zipfile import ZipFile

# FIXME: This should probably be in a config file
DTD_EXPIRY = 60 * 60 * 24 # 1 day
CREDENTIALS = ('benjyjilks@gmail.com', '2n3gfJUdxGizAHF%')
CHUNK_SIZE = 1024 * 1024 # 1MB

Base = declarative_base()

class Metadata(Base):
    __tablename__ = 'metadata'
    last_updated = Column(Integer, primary_key=True)

class LocationRecord(Base):
    __tablename__ = 'location_record'
    uic_code = Column(String(7), primary_key=True)
    ncl_code = Column(String(4))
    crs_code = Column(String(3))

class FlowRecord(Base):
    __tablename__ = 'flow_record'
    flow_id = Column(String(7), primary_key=True)
    origin_code = Column(String(4), ForeignKey('location_record.ncl_code'))
    destination_code = Column(String(4), ForeignKey('location_record.ncl_code'))
    direction = Column(String(1))

class FareRecord(Base):
    __tablename__ = 'fare_record'
    flow_id = Column(String(7), ForeignKey('flow_record.flow_id'), primary_key=True)
    ticket_code = Column(String(3), primary_key=True)
    fare = Column(Integer)

def open_dtd_database() -> Session:
    engine = sqlalchemy.create_engine('sqlite:///dtd.db')
    assert isinstance(engine, Engine)

    Base.metadata.create_all(engine)
    Session = sessionmaker(bind = engine)
    return Session()

def is_dtd_outdated(db: Session) -> bool:
    metadata = db.query(Metadata).first()
    if metadata is None:
        return True

    age = time.time() - metadata.last_updated
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

def download_dtd_zip_file(token: str) -> tuple[str, str]:
    FARES_URL = 'https://opendata.nationalrail.co.uk/api/staticfeeds/2.0/fares'
    HEADERS = { 
        'Content-Type': 'application/json',
        'X-Auth-Token': token,
    }
    response = requests.get(FARES_URL, headers=HEADERS, stream=True)

    disposition = response.headers['Content-Disposition']
    length = int(response.headers['Content-Length'])
    filename = disposition.split(';')[-1].split('=')[-1].strip()[1:-1]
    path = tempfile.mkdtemp()

    print(f"Downloading file '{ filename }' ({ length } bytes)")
    downloaded = 0
    progress = 0
    with open(path + '/' + filename, 'wb') as f:
        for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
            f.write(chunk)
            downloaded += CHUNK_SIZE
            progress += CHUNK_SIZE
            if progress / length >= 0.1:
                print('.', end='')
                sys.stdout.flush()
                progress = 0
    print()
    return path, filename

def record_for_loc_entry(entry: str) -> tuple[type[Base], dict, int] | None:
    entry_type = entry[0:2]
    if entry_type == 'RL':
        uic_code = entry[2:9]
        return LocationRecord, dict(
            uic_code = uic_code,
            ncl_code = entry[36:40], 
            crs_code = entry[56:59]), hash(uic_code)
    return None

def record_for_ffl_entry(entry: str) -> tuple[type[Base], dict, int] | None:
    entry_type = entry[0:2]
    if entry_type == 'RF':
        flow_id = entry[42:49]
        return FlowRecord, dict(
            flow_id = flow_id,
            origin_code = entry[2:6],
            destination_code = entry[6:10],
            direction = entry[19]), hash(flow_id)
    if entry_type == 'RT':
        flow_id = entry[2:9]
        return FareRecord, dict(
            flow_id = flow_id,
            ticket_code = entry[9:12],
            fare = int(entry[12:20])), hash(flow_id)
    return None

def record_for_entry(file: str, entry: str) -> tuple[type[Base], dict, int] | None:
    if file.endswith('LOC'):
        return record_for_loc_entry(entry)
    if file.endswith('FFL'):
        return record_for_ffl_entry(entry)
    return None

def write_dtd_file_to_database(db: Session, path: str, file: str):
    print(f"Writing '{ file }'")
    records: dict[type[Base], tuple[list[dict], set[int]]] = {}
    with open(path + '/' + file, 'r') as f:
        print('  Reading entries')
        for entry in f:
            result = record_for_entry(file, entry)
            if result is None:
                continue

            table, record, hash_value = result
            if not table in records:
                records[table] = [], set()

            table_records, hashes = records[table]
            if not hash_value in hashes:
                table_records.append(record)
                hashes.add(hash_value)
    
    print('  Flushing to database')
    for table, record_set in records.items():
        entries, _ = record_set
        db.bulk_insert_mappings(table, entries)
    db.commit()

def write_dtd_file_set_to_database(db: Session, path: str):
    db.query(Metadata).delete()
    db.query(FareRecord).delete()
    db.query(FlowRecord).delete()
    db.query(LocationRecord).delete()

    for file in os.listdir(path):
        if not file[-3:] in ['LOC', 'FFL']:
            continue
        write_dtd_file_to_database(db, path, file)

    now = int(time.time())
    db.add(Metadata(last_updated = now))
    db.commit()

def update_dtd_database(db: Session):
    if not is_dtd_outdated(db):
        return

    print('Updating DTD database')
    token = generate_dtd_token()
    path, fares_zip = download_dtd_zip_file(token)

    print(f"Extracting '{ fares_zip }'")
    with ZipFile(path + '/' + fares_zip, 'r') as f:
        f.extractall(path)
    os.remove(path + '/' + fares_zip)

    print('Writing to database')
    write_dtd_file_set_to_database(db, path)
    shutil.rmtree(path)

    print('Finished')

