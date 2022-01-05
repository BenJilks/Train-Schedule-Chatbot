import requests
import json
import time
import urllib.parse
import os
import tempfile
import sqlalchemy
import shutil
import datetime
import enum
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from zipfile import ZipFile
from dataclasses import dataclass
from sqlalchemy.engine.base import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import Session, sessionmaker
from sqlalchemy.sql.schema import Column, ForeignKey, Identity
from sqlalchemy.sql.sqltypes import Boolean, Integer, String, Text
from sqlalchemy.sql.sqltypes import Date, Enum, Time

# FIXME: This should probably be in a config file
DATABASE_CONNECTION = 'sqlite:///dtd.db'
# DATABASE_CONNECTION = 'mysql+pymysql://user:password@localhost/db?charset=utf8mb4'
DTD_EXPIRY = 60 * 60 * 24 # 1 day
CREDENTIALS = ('benjyjilks@gmail.com', '2n3gfJUdxGizAHF%')
CHUNK_SIZE = 1024 * 1024 # 1MB

Base = declarative_base()
update_lock = Lock()

class Metadata(Base):
    __tablename__ = 'metadata'
    last_updated = Column(Integer, primary_key=True)

class LocationRecord(Base):
    __tablename__ = 'location_record'
    uic_code = Column(String(7), index=True, primary_key=True)
    ncl_code = Column(String(4), index=True, unique=True)
    crs_code = Column(String(3))

class FlowRecord(Base):
    __tablename__ = 'flow_record'
    flow_id = Column(String(7), index=True, primary_key=True)
    origin_code = Column(String(4), ForeignKey('location_record.ncl_code'))
    destination_code = Column(String(4), ForeignKey('location_record.ncl_code'))
    direction = Column(String(1))

class FareRecord(Base):
    __tablename__ = 'fare_record'
    flow_id = Column(String(7), ForeignKey('flow_record.flow_id'), index=True, primary_key=True)
    ticket_code = Column(String(3), ForeignKey('ticket_type.ticket_code'), index=True, primary_key=True)
    fare = Column(Integer)

class TicketType(Base):
    __tablename__ = 'ticket_type'
    ticket_code = Column(String(3), index=True, primary_key=True)
    description = Column(Text)
    tkt_class = Column(Integer)
    tkt_type = Column(String(1))
    tkt_group = Column(String(1))
    max_passengers = Column(Integer)
    min_passengers = Column(Integer)
    max_adults = Column(Integer)
    min_adults = Column(Integer)
    max_children = Column(Integer)
    min_children = Column(Integer)
    restricted_by_date = Column(Boolean)
    restricted_by_train = Column(Boolean)
    restricted_by_area = Column(Boolean)
    validity_code = Column(String(2))
    reservation_required = Column(String(2))
    capri_code = Column(String(3))
    uts_code = Column(String(2))
    time_restriction = Column(Integer)
    free_pass_lul = Column(Boolean)
    package_mkr = Column(String(1))
    fare_multiplier = Column(Integer)
    discount_category = Column(String(2))

class TrainTimetable(Base):
    __tablename__ = 'train_timetable'
    train_uid = Column(String(6), index=True, primary_key=True)
    date_runs_from = Column(Date)
    date_runs_to = Column(Date)
    days_run = Column(String(7))
    bank_holiday_running = Column(Boolean)

class TimetableLocationType(enum.Enum):
    Origin = enum.auto()
    Intermediate = enum.auto()
    Terminating = enum.auto()

class TimetableLocation(Base):
    __tablename__ = 'timetable_location'
    id = Column(Integer, Identity(start=0), index=True, primary_key=True)
    train_uid = Column(String(6), ForeignKey('train_timetable.train_uid'), index=True)
    train_route_index = Column(Integer, index=True)
    location_type = Column(Enum(TimetableLocationType))
    location = Column(String(8), ForeignKey('tiploc.tiploc_code'))
    scheduled_arrival_time = Column(Time)
    scheduled_departure_time = Column(Time)
    scheduled_pass = Column(Time)
    public_arrival = Column(Time)
    public_departure = Column(Time)
    platform = Column(String(3))
    line = Column(String(3))
    path = Column(String(3))
    activity = Column(String(12))
    engineering_allowance = Column(String(2))
    pathing_allowance = Column(String(2))
    performance_allowance = Column(String(2))

class TIPLOC(Base):
    __tablename__ = 'tiploc'
    id = Column(Integer, Identity(start=0), index=True, primary_key=True)
    tiploc_code = Column(String(7), index=True, unique=True)
    crs_code = Column(String(3))
    description = Column(Text)

def open_dtd_database() -> Session:
    engine = sqlalchemy.create_engine(DATABASE_CONNECTION)
    assert isinstance(engine, Engine)

    Base.metadata.create_all(engine)
    db = sessionmaker(bind = engine)()
    update_dtd_database(db)
    return db

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

def download_dtd_zip_file(token: str, category: str) -> tuple[str, str]:
    FARES_URL = 'https://opendata.nationalrail.co.uk/api/staticfeeds/' + category
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
    with open(path + '/' + filename, 'wb') as f:
        for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
            f.write(chunk)

    print(f"Finished downloading '{ filename }'")
    return path, filename

def record_for_loc_entry(entry: str) -> tuple[type[Base], dict, int] | None:
    entry_type = entry[:2]
    if entry_type == 'RL':
        uic_code = entry[2:9]
        return LocationRecord, dict(
            uic_code = uic_code,
            ncl_code = entry[36:40], 
            crs_code = entry[56:59]), hash(uic_code)
    return None

def record_for_ffl_entry(entry: str) -> tuple[type[Base], dict, int] | None:
    entry_type = entry[:2]
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

def record_for_tty_entry(entry: str) -> tuple[type[Base], dict, int] | None:
    entry_type = entry[:1]
    if entry_type == 'R':
        ticket_code = entry[1:4]
        return TicketType, dict(
            ticket_code = ticket_code,
            description = entry[28:43].strip(),
            tkt_class = int(entry[43]),
            tkt_type = entry[44],
            tkt_group = entry[45],
            max_passengers = int(entry[54:57]),
            min_passengers = int(entry[57:60]),
            max_adults = int(entry[60:63]),
            min_adults = int(entry[63:66]),
            max_children = int(entry[66:69]),
            min_children = int(entry[69:72]),
            restricted_by_date = entry[72] == 'Y',
            restricted_by_train = entry[73] == 'Y',
            restricted_by_area = entry[74] == 'Y',
            validity_code = entry[75:77],
            reservation_required = entry[98],
            capri_code = entry[99:102],
            uts_code = entry[103:105],
            time_restriction = int(entry[105]),
            free_pass_lul = entry[106] == 'Y',
            package_mkr = entry[107],
            fare_multiplier = int(entry[108:111]),
            discount_category = entry[111:113]), hash(ticket_code)
    return None

def parse_time(time_str: str) -> datetime.time:
    assert len(time_str) >= 4
    hour_str = time_str[:2]
    minute_str = time_str[2:4]
    hour = 0 if hour_str == '  ' else int(hour_str)
    minute = 0 if minute_str == '  ' else int(minute_str)
    return datetime.time(hour=hour, minute=minute)

def parse_date(date_str: str) -> datetime.date:
    assert len(date_str) >= 6
    year = 2000 + int(date_str[:2])
    month = int(date_str[2:4])
    day = int(date_str[4:6])
    return datetime.date(year, month, day)

@dataclass
class State:
    current_train: str | None = None
    train_route_index: int = 0

    def reset(self):
        self.current_train = None
        self.train_route_index = 0

def record_for_mca_entry(entry: str, state: State) -> tuple[type[Base], dict, int] | None:
    entry_type = entry[:2]
    if entry_type == 'BS':
        train_uid = entry[3:9]
        state.current_train = train_uid
        return TrainTimetable, dict(
            train_uid = train_uid,
            date_runs_from = parse_date(entry[9:15]),
            date_runs_to = parse_date(entry[15:21]),
            days_run = entry[21:28],
            bank_holiday_running = (entry[28] == 'Y')), hash(train_uid)
    if entry_type == 'LO':
        assert isinstance(state.current_train, str)
        state.train_route_index += 1
        train_uid = state.current_train
        location = entry[2:10].strip()
        return TimetableLocation, dict(
            train_uid = train_uid,
            train_route_index = state.train_route_index - 1,
            location_type = TimetableLocationType.Origin,
            location = location,
            scheduled_departure_time = parse_time(entry[10:15]),
            public_departure = parse_time(entry[15:19]),
            platform = entry[19:22].strip(),
            line = entry[22:25].strip(),
            engineering_allowance = entry[25:27].strip(),
            pathing_allowance = entry[27:29].strip(),
            activity = entry[39:41].strip(),
            performance_allowance = entry[41:43].strip()), hash((train_uid, location))
    if entry_type == 'LI':
        assert isinstance(state.current_train, str)
        state.train_route_index += 1
        train_uid = state.current_train
        location = entry[2:10].strip()
        return TimetableLocation, dict(
            train_uid = train_uid,
            train_route_index = state.train_route_index - 1,
            location_type = TimetableLocationType.Intermediate,
            location = location,
            scheduled_arrival_time = parse_time(entry[10:15]),
            scheduled_departure_time = parse_time(entry[15:20]),
            scheduled_pass = parse_time(entry[20:25]),
            public_arrival = parse_time(entry[25:29]),
            public_departure = parse_time(entry[29:33]),
            platform = entry[33:36].strip(),
            line = entry[36:39].strip(),
            path = entry[39:42].strip(),
            activity = entry[42:54].strip(),
            engineering_allowance = entry[54:56].strip(),
            pathing_allowance = entry[56:58].strip(),
            performance_allowance = entry[58:60].strip()), hash((train_uid, location))
    if entry_type == 'LT':
        assert isinstance(state.current_train, str)
        train_uid = state.current_train
        train_route_index = state.train_route_index
        location = entry[2:10].strip()
        state.reset()
        return TimetableLocation, dict(
            train_uid = train_uid,
            train_route_index = train_route_index,
            location_type = TimetableLocationType.Terminating,
            location = location,
            scheduled_arrival_time = parse_time(entry[10:15]),
            public_arrival = parse_time(entry[15:19]),
            platform = entry[19:22].strip(),
            path = entry[22:25].strip(),
            activity = entry[25:37].strip()), hash((train_uid, location))
    if entry_type == 'TI':
        return TIPLOC, dict(
            tiploc_code = entry[2:9].strip(),
            crs_code = entry[53:56],
            description = entry[56:72].strip()), hash(entry)
    return None

def record_for_entry(file: str, entry: str, state: State) -> tuple[type[Base], dict, int] | None:
    if file.endswith('LOC'):
        return record_for_loc_entry(entry)
    if file.endswith('FFL'):
        return record_for_ffl_entry(entry)
    if file.endswith('TTY'):
        return record_for_tty_entry(entry)
    if file.endswith('MCA'):
        return record_for_mca_entry(entry, state)
    return None

def records_in_dtd_file(path: str, file: str) -> dict[type[Base], list[dict]]:
    print(f"Reading '{ file }'")
    records: dict[type[Base], tuple[list[dict], set[int]]] = {}
    with open(path + '/' + file, 'r') as f:
        state = State()
        for entry in f:
            result = record_for_entry(file, entry, state)
            if result is None:
                continue

            table, record, hash_value = result
            if not table in records:
                records[table] = [], set()

            table_records, hashes = records[table]
            if not hash_value in hashes:
                table_records.append(record)
                hashes.add(hash_value)
    return { table: entries for table, (entries, _) in records.items() }

def records_in_dtd_file_set(executor: ThreadPoolExecutor, path: str):
    tasks = []
    for file in os.listdir(path):
        if not file[-3:] in ['LOC', 'FFL', 'TTY', 'MCA']:
            continue
        tasks.append(executor.submit(
            lambda *args: (*args, records_in_dtd_file(*args)), path, file))
    return tasks

def download_dtd_category(token: str, category: str) -> str:
    path, zip_file = download_dtd_zip_file(token, category)

    print(f"Extracting '{ zip_file }'")
    with ZipFile(path + '/' + zip_file, 'r') as f:
        f.extractall(path)
    os.remove(path + '/' + zip_file)
    return path

def clear_dtd_database(db: Session):
    for table in Base.metadata.sorted_tables:
        db.query(table).delete()

def update_dtd_database(db: Session):
    global update_lock
    update_lock.acquire()

    if not is_dtd_outdated(db):
        update_lock.release()
        return

    print('Updating DTD database')
    token = generate_dtd_token()

    with ThreadPoolExecutor(max_workers=5) as executor:
        download_tasks = []
        for category in ['2.0/fares', '3.0/timetable']:
            download_tasks.append(executor.submit(download_dtd_category, token, category))

        # Clear alongside downloading
        clear_dtd_database(db)

        write_tasks = []
        paths = []
        for task in as_completed(download_tasks):
            path = task.result()
            write_tasks += records_in_dtd_file_set(executor, path)
            paths.append(path)

        for task in as_completed(write_tasks):
            path, file, records = task.result()

            print(f"Flushing '{ file }' to database")
            for table, entries in records.items():
                db.bulk_insert_mappings(table, entries)
            db.commit()

        # Clean up /tmp directory
        for path in paths:
            shutil.rmtree(path)

    now = int(time.time())
    db.add(Metadata(last_updated = now))
    db.commit()
    update_lock.release()
    print('Finished')

