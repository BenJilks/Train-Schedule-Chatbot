import sys
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
import traceback
from queue import Queue
from typing import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor, Executor
from concurrent.futures import as_completed, wait, FIRST_EXCEPTION
from zipfile import ZipFile
from dataclasses import dataclass, field
from sqlalchemy.engine.base import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import Session, sessionmaker
from sqlalchemy.sql.schema import Column, ForeignKey, Identity
from sqlalchemy.sql.sqltypes import Boolean, Integer, String, Text
from sqlalchemy.sql.sqltypes import Date, Enum, Time

from knowledge_base.progress import Progress

# FIXME: This should probably be in a config file
DATABASE_FILE = 'dtd.db'
DTD_EXPIRY = 60 * 60 * 24 # 1 day
CREDENTIALS = ('benjyjilks@gmail.com', '2n3gfJUdxGizAHF%')

DOWNLOAD_CHUNK_SIZE = 1024 * 1024 # 1MB
MAX_NUMBER_OF_QUEUED_BATCH_STATEMENTS = 5
RECORD_CHUNK_SIZE = 1_00_000
SQL_BATCH_SIZE = 10_00_000

Base = declarative_base()
is_updating = False

class Metadata(Base):
    __tablename__ = 'metadata'
    last_updated = Column(Integer, primary_key=True)

class LocationRecord(Base):
    __tablename__ = 'location_record'
    id = Column(Integer, Identity(start=0), primary_key=True)
    uic_code = Column(String(7))
    ncl_code = Column(String(4), index=True)
    crs_code = Column(String(3), index=True)

class FlowRecord(Base):
    __tablename__ = 'flow_record'
    flow_id = Column(Integer, index=True, primary_key=True)
    origin_code = Column(String(4), ForeignKey('location_record.ncl_code'))
    destination_code = Column(String(4), ForeignKey('location_record.ncl_code'))
    direction = Column(String(1))
    end_date = Column(Date)
    start_date = Column(Date)

class FareRecord(Base):
    __tablename__ = 'fare_record'
    flow_id = Column(Integer, ForeignKey('flow_record.flow_id'), index=True, primary_key=True)
    ticket_code = Column(String(3), ForeignKey('ticket_type.ticket_code'), primary_key=True)
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
    id = Column(Integer, Identity(start=0), primary_key=True)
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
    id = Column(Integer, Identity(start=0), primary_key=True)
    tiploc_code = Column(String(7), index=True, unique=True)
    crs_code = Column(String(3))
    description = Column(Text)

def open_dtd_database() -> Session:
    is_new_database = not os.path.exists(DATABASE_FILE)
    engine = sqlalchemy.create_engine('sqlite:///' + DATABASE_FILE)
    assert isinstance(engine, Engine)

    Base.metadata.create_all(engine)
    db = sessionmaker(bind = engine)()
    if is_new_database:
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('PRAGMA synchronous = NORMAL')
        db.execute('PRAGMA cache_size = 100000')

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

def download_dtd_zip_file(token: str, category: str, progress: Progress) -> tuple[str, str]:
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

    bytes_downloaded = 0
    last_progress_report = 0
    with open(path + '/' + filename, 'wb') as f:
        for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
            f.write(chunk)
            bytes_downloaded += DOWNLOAD_CHUNK_SIZE
            if time.time() - last_progress_report >= 1:
                progress.report(filename, bytes_downloaded, length)
                last_progress_report = time.time()

    progress.report(filename, length, length)
    return path, filename

def parse_time(time_str: str) -> datetime.time:
    assert len(time_str) >= 4
    hour_str = time_str[:2]
    minute_str = time_str[2:4]
    hour = 0 if hour_str == '  ' else int(hour_str)
    minute = 0 if minute_str == '  ' else int(minute_str)
    return datetime.time(hour=hour, minute=minute)

def parse_date_yymmdd(date_str: str) -> datetime.date:
    assert len(date_str) >= 6
    year = 2000 + int(date_str[:2])
    month = int(date_str[2:4])
    day = int(date_str[4:6])
    return datetime.date(year, month, day)

def parse_date_ddmmyyyy(date_str: str) -> datetime.date:
    assert len(date_str) >= 8
    day = int(date_str[:2])
    month = int(date_str[2:4])
    year = int(date_str[4:8])
    return datetime.date(year, month, day)

@dataclass
class State:
    current_train: dict | None = None
    train_route_index: int = 0
    has_terminated: bool = False

    expired_flow_ids: set[int] = field(default_factory=set)
    duplicate_trains: set[int] = field(default_factory=set)

    def reset(self):
        self.current_train = None
        self.train_route_index = 0
        self.has_terminated = False

def record_for_loc_entry(entry: str, _: State) -> Iterator[tuple[type[Base], dict]]:
    entry_type = entry[:2]
    if entry_type == 'RL':
        yield LocationRecord, dict(
            uic_code = entry[2:9],
            ncl_code = entry[36:40], 
            crs_code = entry[56:59])
    return

def has_entry_expired(start: datetime.date, end: datetime.date) -> bool:
    if datetime.date.today() < start:
        return True

    # NOTE: This means there is no end date
    if end.year >= 2999:
        return False
    if datetime.date.today() > end:
        return True
    return False

Record = tuple[type[Base], dict]
RecordSet = dict[type[Base], list[dict]]

def record_for_ffl_entry(entry: str, state: State) -> Iterator[Record]:
    entry_type = entry[:2]
    if entry_type == 'RF':
        flow_id = int(entry[42:49])
        end_date = parse_date_ddmmyyyy(entry[20:28])
        start_date = parse_date_ddmmyyyy(entry[28:36])
        if has_entry_expired(start_date, end_date):
            state.expired_flow_ids.add(flow_id)
            return

        yield FlowRecord, dict(
            flow_id = flow_id,
            origin_code = entry[2:6],
            destination_code = entry[6:10],
            direction = entry[19],
            end_date = end_date,)

    elif entry_type == 'RT':
        flow_id = int(entry[2:9])
        if flow_id in state.expired_flow_ids:
            return

        yield FareRecord, dict(
            flow_id = flow_id,
            ticket_code = entry[9:12],
            fare = int(entry[12:20]))

    return

def record_for_tty_entry(entry: str, _: State) -> Iterator[Record]:
    entry_type = entry[:1]
    if entry_type == 'R':
        end_date = parse_date_ddmmyyyy(entry[4:12])
        start_date = parse_date_ddmmyyyy(entry[12:20])
        if has_entry_expired(start_date, end_date):
            return

        yield TicketType, dict(
            ticket_code = entry[1:4],
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
            discount_category = entry[111:113])

    return

def record_for_mca_entry(entry: str, state: State) -> Iterator[Record]:
    entry_type = entry[:2]
    if entry_type == 'BS':
        state.reset()
        state.current_train = dict(
            train_uid = entry[3:9],
            date_runs_from = parse_date_yymmdd(entry[9:15]),
            date_runs_to = parse_date_yymmdd(entry[15:21]),
            days_run = entry[21:28],
            bank_holiday_running = (entry[28] == 'Y'))

    elif entry_type == 'LO':
        assert not state.current_train is None
        assert not state.has_terminated
        state.train_route_index += 1
        yield TimetableLocation, dict(
            train_uid = state.current_train['train_uid'],
            train_route_index = state.train_route_index - 1,
            location_type = TimetableLocationType.Origin,
            location = entry[2:10].strip(),
            scheduled_departure_time = parse_time(entry[10:15]),
            public_departure = parse_time(entry[15:19]),
            platform = entry[19:22].strip(),
            line = entry[22:25].strip(),
            engineering_allowance = entry[25:27].strip(),
            pathing_allowance = entry[27:29].strip(),
            activity = entry[39:41].strip(),
            performance_allowance = entry[41:43].strip())

    elif entry_type == 'LI':
        assert not state.current_train is None
        assert not state.has_terminated
        state.train_route_index += 1
        yield TimetableLocation, dict(
            train_uid = state.current_train['train_uid'],
            train_route_index = state.train_route_index - 1,
            location_type = TimetableLocationType.Intermediate,
            location = entry[2:10].strip(),
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
            performance_allowance = entry[58:60].strip())

    elif entry_type == 'LT':
        assert not state.current_train is None
        assert not state.has_terminated
        state.has_terminated = True

        train_uid = state.current_train['train_uid']
        if state.duplicate_trains.add(train_uid):
            yield TrainTimetable, state.current_train

        yield TimetableLocation, dict(
            train_uid = train_uid,
            train_route_index = state.train_route_index,
            location_type = TimetableLocationType.Terminating,
            location = entry[2:10].strip(),
            scheduled_arrival_time = parse_time(entry[10:15]),
            public_arrival = parse_time(entry[15:19]),
            platform = entry[19:22].strip(),
            path = entry[22:25].strip(),
            activity = entry[25:37].strip())

    elif entry_type == 'TI':
        yield TIPLOC, dict(
            tiploc_code = entry[2:9].strip(),
            crs_code = entry[53:56],
            description = entry[56:72].strip())

    return

def entry_parser_for_file(file: str) -> Callable[[str, State], Iterator[Record]]:
    if file.endswith('LOC'):
        return record_for_loc_entry
    if file.endswith('FFL'):
        return record_for_ffl_entry
    if file.endswith('TTY'):
        return record_for_tty_entry
    if file.endswith('MCA'):
        return record_for_mca_entry
    raise Exception(f"Unkown file '{ file }'")

def records_in_dtd_file(chunk_queue: Queue[RecordSet], path: str, file: str, 
                        progress: Progress):
    record_chunk: RecordSet = {}
    record_chunk_count = 0

    last_progress_report = 0
    total_size_bytes = os.path.getsize(path + '/' + file)
    bytes_processed = 0

    entry_parser = entry_parser_for_file(file)
    with open(path + '/' + file, 'r') as f:
        state = State()
        line_no = 0
        for entry_line in f:
            line_no += 1
            bytes_processed += len(entry_line)
            if time.time() - last_progress_report >= 1:
                progress.report(file, bytes_processed, total_size_bytes)
                last_progress_report = time.time()

            for table, entry in entry_parser(entry_line, state):
                record_chunk.setdefault(table, []).append(entry)
                record_chunk_count += 1

            if record_chunk_count < RECORD_CHUNK_SIZE:
                continue
            chunk_queue.put(record_chunk)
            record_chunk = {}
            record_chunk_count = 0

    if record_chunk_count > 0:
        chunk_queue.put(record_chunk)
    progress.report(file, total_size_bytes, total_size_bytes)

def records_in_dtd_file_set(executor: Executor, chunk_queue: Queue[RecordSet | None],
                            path: str, progress: Progress):
    tasks = []
    for file in os.listdir(path):
        if not file[-3:] in ['LOC', 'FFL', 'TTY', 'MCA']:
            continue
        tasks.append(executor.submit(records_in_dtd_file, chunk_queue, path, file, progress))

    return tasks

def download_dtd_category(token: str, category: str, progress: Progress) -> str:
    path, zip_file = download_dtd_zip_file(token, category, progress)
    with ZipFile(path + '/' + zip_file, 'r') as f:
        f.extractall(path)
    os.remove(path + '/' + zip_file)
    return path

def clear_dtd_database(db: Session):
    for table in Base.metadata.sorted_tables:
        db.query(table).delete()

def report_flushing_progress(progress: Progress, 
                             written: int, chunk: int, queue_size: int):
    progress.report('Writing to Disk', 
        written, 
        written + chunk + queue_size*RECORD_CHUNK_SIZE)

def flush_record_chunk(db: Session, record_chunk: RecordSet, 
                       chunk_count: int, written: int, 
                       queue_size: int, progress: Progress):
    report_flushing_progress(progress, written, chunk_count, queue_size)

    for table, entries in record_chunk.items():
        db.bulk_insert_mappings(table, entries)
    
    report_flushing_progress(progress, written + chunk_count, 0, queue_size)

def batch_and_flush_chunks(db: Session, chunk_queue: Queue[RecordSet | None],
                           progress: Progress):
    current_chunk: RecordSet = {}
    current_chunk_count = 0
    total_records_being_written = 0
    for record_chunk in iter(chunk_queue.get, None):
        for table, entities in record_chunk.items():
            current_chunk.setdefault(table, []).extend(entities)
            current_chunk_count += len(entities)

        report_flushing_progress(progress, 
            total_records_being_written, current_chunk_count, chunk_queue.qsize())

        # Wait for record batch to fill up
        if current_chunk_count < SQL_BATCH_SIZE:
            continue

        flush_record_chunk(db, current_chunk, 
            current_chunk_count, total_records_being_written, 
            chunk_queue.qsize(), progress)

        total_records_being_written += current_chunk_count
        current_chunk = {}
        current_chunk_count = 0

    if current_chunk_count > 0:
        flush_record_chunk(db, current_chunk, 
            current_chunk_count, total_records_being_written, 
            chunk_queue.qsize(), progress)
    report_flushing_progress(progress, 0, 0, 0)

def create_new_table(db: Session, executor: Executor):
    token = generate_dtd_token()
    progress = Progress()

    download_tasks = []
    for category in ['2.0/fares', '3.0/timetable']:
        download_tasks.append(executor.submit(download_dtd_category, token, category, progress))

    # Clear alongside downloading
    clear_dtd_database(db)

    max_queue_size = int(SQL_BATCH_SIZE / RECORD_CHUNK_SIZE) * MAX_NUMBER_OF_QUEUED_BATCH_STATEMENTS
    chunk_queue: Queue[RecordSet | None] = Queue(maxsize = max_queue_size)

    write_tasks = []
    paths = []
    for task in as_completed(download_tasks):
        path = task.result()
        paths.append(path) 
        write_tasks += records_in_dtd_file_set(
            executor, chunk_queue, path, progress)

    # Write each chunk synchronously on the main thread
    def terminate_queue_on_tasks_complete():
        try:
            wait(write_tasks, return_when=FIRST_EXCEPTION)
        except Exception as e:
            print(Exception, e, file=sys.stderr)
            traceback.print_exc(file=sys.stderr)

            chunk_queue.put(None)
            raise e
        chunk_queue.put(None)
    wait_task = executor.submit(terminate_queue_on_tasks_complete)

    # NOTE: We can only run SQL on the main thread
    batch_and_flush_chunks(db, chunk_queue, progress)

    # Propagate any exceptions
    wait([wait_task], return_when=FIRST_EXCEPTION)

    # Clean up /tmp directory
    for path in paths:
        shutil.rmtree(path)

def update_dtd_database(db: Session):
    global is_updating
    if is_updating:
        return

    if not is_dtd_outdated(db):
        return

    print('Updating DTD database')
    is_updating = True

    with ThreadPoolExecutor() as executor:
        try:
            create_new_table(db, executor)
        except Exception as e:
            print(Exception, e, file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            raise e

    now = int(time.time())
    db.add(Metadata(last_updated = now))
    db.commit()
    is_updating = False
    print('Finished')

