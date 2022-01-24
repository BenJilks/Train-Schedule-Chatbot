from __future__ import annotations
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
import traceback
import config
from typing import Iterable, TextIO, Union
from abc import ABC, abstractmethod
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import Integer, Text
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, Executor, Future
from concurrent.futures import as_completed, wait, FIRST_EXCEPTION
from sqlalchemy.engine.base import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import Session, sessionmaker
from knowledge_base.progress import Progress

Base = declarative_base()
is_updating = False

Record = tuple[type[Base], dict]
RecordSet = dict[type[Base], list[dict]]

class ExpiryTimes(Base):
    __tablename__ = 'expiry_times'
    api_url = Column(Text, primary_key=True)
    expiry_timestamp = Column(Integer)

class RecordChunkGenerator:
    _chunk_queue: Queue[RecordSet]
    _chunk: RecordSet
    _chunk_count: int

    def __init__(self, chunk_queue: Queue[RecordSet]):
        self._chunk_queue = chunk_queue
        self._chunk = {}
        self._chunk_count = 0

    def put(self, record: Record):
        table, entry = record
        self._chunk.setdefault(table, []).append(entry)
        self._chunk_count += 1

        if self._chunk_count >= config.RECORD_CHUNK_SIZE:
            self._chunk_queue.put(self._chunk)
            self._chunk = {}
            self._chunk_count = 0

    def __enter__(self):
        return self
    
    def __exit__(self, *_):
        if self._chunk_count > 0:
            self._chunk_queue.put(self._chunk)

class Feed(ABC):
    _registered_feeds: set[type[Feed]] = set()

    @abstractmethod
    def associated_tables(self) -> Iterable[type[Base]]:
        ...

    @abstractmethod
    def expiry_length(self) -> int:
        ...

    @abstractmethod
    def file_name(self) -> str:
        ...

    @abstractmethod
    def feed_api_url(self) -> str:
        ...

    @abstractmethod
    def records_in_feed(self,
                        executor: Executor,
                        chunk_queue: Queue[Union[RecordSet, None]],
                        path: str,
                        progress: Progress) -> Iterable[Future]:
        ...
    
    def preprocess_hook(self, _: Session):
        pass

    def unique_path_id(self) -> str:
        return str(hash(self.feed_api_url()))

    @staticmethod
    def register(feed: type[Feed]):
        Feed._registered_feeds.add(feed)

    @staticmethod
    def feeds() -> list[Feed]:
        return [feed() for feed in Feed._registered_feeds]

def open_database(file: TextIO = sys.stdout) -> Session:
    is_new_database = not os.path.exists(config.DATABASE_FILE)
    engine = sqlalchemy.create_engine('sqlite:///' + config.DATABASE_FILE)
    assert isinstance(engine, Engine)

    Base.metadata.create_all(engine)
    db = sessionmaker(bind = engine)()
    if is_new_database:
        db.execute('PRAGMA journal_mode = WAL')
        db.execute('PRAGMA synchronous = NORMAL')
        db.execute('PRAGMA cache_size = 100000')

    update_database(db, file)
    return db

def generate_opendata_token() -> str:
    AUTHENTICATE_URL = 'https://opendata.nationalrail.co.uk/authenticate'
    HEADERS = { 'Content-Type': 'application/x-www-form-urlencoded' }

    response = requests.post(AUTHENTICATE_URL, headers=HEADERS, 
        data=f"username={ config.CREDENTIALS[0] }&password={ urllib.parse.quote_plus(config.CREDENTIALS[1]) }")

    response_json = json.loads(response.text)
    return response_json['token']

def feed_file_from_storage(data_path: str, feed: Feed) -> tuple[Feed, str]:
    storage_path = config.LOCAL_FEED_STORAGE_BASE
    file_name = feed.file_name()
    working_path = os.path.join(data_path, str(hash(feed.feed_api_url())))
    os.makedirs(working_path, exist_ok=True)

    shutil.copy(
        os.path.join(storage_path, file_name),
        os.path.join(working_path, file_name))
    return feed, working_path

def download_feed_file(token: str, data_path: str, feed: Feed,
                       progress: Progress) -> tuple[Feed, str]:
    if config.DISABLE_DOWNLOAD:
        return feed_file_from_storage(data_path, feed)

    FARES_URL = 'https://opendata.nationalrail.co.uk/api/staticfeeds/' + feed.feed_api_url()
    HEADERS = { 
        'Content-Type': 'application/json',
        'X-Auth-Token': token,
    }
    response = requests.get(FARES_URL, headers=HEADERS, stream=True)

    length = 0
    if 'Content-Length' in response.headers:
        length = int(response.headers['Content-Length'])

    file_name = feed.file_name()
    path = os.path.join(data_path, feed.unique_path_id())
    os.makedirs(path)

    bytes_downloaded = 0
    last_progress_report = 0
    with open(os.path.join(path, file_name), 'wb') as f:
        for chunk in response.iter_content(chunk_size=config.DOWNLOAD_CHUNK_SIZE):
            f.write(chunk)
            bytes_downloaded += config.DOWNLOAD_CHUNK_SIZE
            if time.time() - last_progress_report >= 1:
                progress.report(file_name, bytes_downloaded, length)
                last_progress_report = time.time()

    progress.report(file_name, length, length)
    return feed, path

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

def time_to_sql(time: datetime.time) -> int:
    return time.hour*100 + time.minute

def time_from_sql(time: int) -> datetime.time:
    return datetime.time(time // 100, time % 100)

def date_to_sql(date: datetime.date) -> int:
    return date.year*10000 + date.month*100 + date.day

def date_from_sql(date: int) -> datetime.date:
    return datetime.date(date // 10000, (date // 100) % 100, date % 100)

def report_flushing_progress(progress: Progress, 
                             written: int, chunk: int, queue_size: int):
    progress.report('Writing to Disk', 
        written, 
        written + chunk + queue_size*config.RECORD_CHUNK_SIZE)

def flush_record_chunk(db: Session, record_chunk: RecordSet, 
                       chunk_count: int, written: int, 
                       queue_size: int, progress: Progress):
    report_flushing_progress(progress, written, chunk_count, queue_size)

    for table, entries in record_chunk.items():
        db.bulk_insert_mappings(table, entries)
    db.commit()
    
    report_flushing_progress(progress, written + chunk_count, 0, queue_size)


def batch_and_flush_chunks(db: Session, chunk_queue: Queue[Union[RecordSet, None]],
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
        if current_chunk_count < config.SQL_BATCH_SIZE:
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

def backup_feed_file_to_storage(path: str, feed: Feed):
    storage_path = config.LOCAL_FEED_STORAGE_BASE
    file_name = feed.file_name()
    os.makedirs(storage_path, exist_ok=True)
    shutil.copy(
        os.path.join(path, file_name),
        os.path.join(storage_path, file_name))

def update_feeds(db: Session, executor: Executor, feeds: Iterable[Feed], file: TextIO):
    token = '' if config.DISABLE_DOWNLOAD else generate_opendata_token()
    progress = Progress(file)
    data_path = tempfile.mkdtemp()

    download_tasks: list[Future[tuple[Feed, str]]] = []
    for feed in feeds:
        download_tasks.append(executor.submit(download_feed_file,
            token, data_path, feed, progress))

        # Clear alongside downloading
        for table in feed.associated_tables():
            db.query(table).delete()

    chunk_queue: Queue[Union[RecordSet, None]] = Queue(maxsize = config.MAX_QUEUE_SIZE)
    write_tasks: list[Future] = []
    for task in as_completed(download_tasks):
        feed, path = task.result()

        # Copy to local storage if enabled
        if config.BACKUP_DOWNLOADED_TO_LOCAL:
            backup_feed_file_to_storage(path, feed)

        write_tasks += feed.records_in_feed(
            executor, chunk_queue, path, progress)

    # Write each chunk synchronously on the main thread
    def terminate_queue_on_tasks_complete(tasks: Iterable[Future]):
        for result in as_completed(tasks):
            e = result.exception()
            if not e:
                continue

            print(result, file=sys.stderr)
            print(type(e), e, file=sys.stderr)
            traceback.print_exception(type(e), value=e, file=sys.stderr)

            chunk_queue.put(None)
            raise e
        chunk_queue.put(None)
    wait_task = executor.submit(terminate_queue_on_tasks_complete, write_tasks)

    # NOTE: We can only run SQL on the main thread
    batch_and_flush_chunks(db, chunk_queue, progress)
    for feed in feeds:
        feed.preprocess_hook(db)

    # Propagate any exceptions
    wait([wait_task], return_when=FIRST_EXCEPTION)

    # Clean up /tmp directory
    shutil.rmtree(data_path)

def get_outdated_feeds(db: Session) -> list[Feed]:
    now = int(time.time())
    not_expired = [value[0]
        for value in db.query(ExpiryTimes.api_url)\
            .filter(now < ExpiryTimes.expiry_timestamp)\
            .all()]

    return [feed
        for feed in Feed.feeds()
        if not feed.feed_api_url() in not_expired]

def update_expiry_times(db: Session, feeds: Iterable[Feed]):
    for feed in feeds:
        now = int(time.time())
        db.merge(ExpiryTimes(
            api_url = feed.feed_api_url(),
            expiry_timestamp = now + feed.expiry_length()))
    db.commit()

def update_database(db: Session, file: TextIO):
    global is_updating
    if is_updating:
        return

    outdated_feeds = get_outdated_feeds(db)
    if len(outdated_feeds) == 0:
        return

    print('Updating feeds', *[feed.feed_api_url() for feed in outdated_feeds], file=file)
    is_updating = True

    with ThreadPoolExecutor() as executor:
        try:
            update_feeds(db, executor, outdated_feeds, file)
        except Exception as e:
            print(Exception, e, file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            raise e

    # Update expiry times
    update_expiry_times(db, outdated_feeds)

    is_updating = False
    print('Finished')

