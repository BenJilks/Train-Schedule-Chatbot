import time
import os
import datetime
import enum
from zipfile import ZipFile
from sqlalchemy.orm.util import aliased
from queue import Queue
from typing import Callable, Iterable
from concurrent.futures import Executor, Future
from dataclasses import dataclass, field
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import Boolean, Integer, String, Text
from sqlalchemy.sql.sqltypes import Date, Enum, Time
from knowledge_base.feeds import Base, Feed, Record, RecordChunkGenerator, RecordSet
from knowledge_base.feeds import date_to_sql, time_to_sql
from knowledge_base.feeds import parse_date_ddmmyyyy, parse_date_yymmdd, parse_time
from knowledge_base.progress import Progress

class LocationRecord(Base):
    __tablename__ = 'location_record'
    crs_code = Column(String(3), primary_key=True)
    ncl_code = Column(String(4), unique=True)
    uic_code = Column(String(7))

class StationCluster(Base):
    __tablename__ = 'station_cluster'
    cluster_id = Column(String(4), index=True, primary_key=True)
    location_nlc = Column(String(4), index=True, primary_key=True)

class FlowRecord(Base):
    __tablename__ = 'flow_record'
    flow_id = Column(Integer, primary_key=True)
    origin_code = Column(String(4), ForeignKey('location_record.ncl_code'), index=True)
    destination_code = Column(String(4), ForeignKey('location_record.ncl_code'), index=True)
    direction = Column(String(1))
    toc = Column(String(2))
    end_date = Column(Date)
    start_date = Column(Date)

class FareRecord(Base):
    __tablename__ = 'fare_record'
    flow_id = Column(Integer, ForeignKey('flow_record.flow_id'), primary_key=True)
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
    date_runs_from = Column(Integer)
    date_runs_to = Column(Integer)
    monday = Column(Boolean)
    tuesday = Column(Boolean)
    wednesday = Column(Boolean)
    thursday = Column(Boolean)
    friday = Column(Boolean)
    saturday = Column(Boolean)
    sunday = Column(Boolean)
    bank_holiday_running = Column(Boolean)
    rsid = Column(String(8))
    toc = Column(String(2))

    @staticmethod
    def day_to_column(day: int) -> Column:
        day_index = [
            TrainTimetable.monday,
            TrainTimetable.tuesday,
            TrainTimetable.wednesday,
            TrainTimetable.thursday,
            TrainTimetable.friday,
            TrainTimetable.saturday,
            TrainTimetable.sunday,
        ]
        return day_index[day]

class TimetableLocationType(enum.Enum):
    Origin = enum.auto()
    Intermediate = enum.auto()
    Terminating = enum.auto()

class TimetableLocation(Base):
    __tablename__ = 'timetable_location'
    train_uid = Column(String(6), ForeignKey('train_timetable.train_uid'), index=True, primary_key=True)
    train_route_index = Column(Integer, index=True, primary_key=True)
    location_type = Column(Enum(TimetableLocationType))
    location = Column(String(7), ForeignKey('tiploc.tiploc_code'), index=True)
    scheduled_arrival_time = Column(Integer)
    scheduled_departure_time = Column(Integer)
    public_arrival = Column(Time)
    public_departure = Column(Time)
    platform = Column(String(3))
    line = Column(String(3))
    path = Column(String(3))
    activity = Column(String(12))
    engineering_allowance = Column(String(2))
    pathing_allowance = Column(String(2))
    performance_allowance = Column(String(2))

class TimetableLink(Base):
    __tablename__ = 'timetable_link'
    from_location = Column(String(7), index=True, primary_key=True)
    to_location = Column(String(7), index=True, primary_key=True)

class TIPLOC(Base):
    __tablename__ = 'tiploc'
    tiploc_code = Column(String(7), primary_key=True)
    crs_code = Column(String(3), primary_key=True)
    description = Column(Text)

"""
class Station(Base):
    __tablename__ = 'station'
    location_crs = Column(String(3), index=True, primary_key=True)
    station_group_id = Column(String(3))
    node_id = Column(String(3))

class StationLink(Base):
    __tablename__ = 'station_link'
    start_station = Column(String(3), index=True, primary_key=True)
    end_station = Column(String(3), index=True, primary_key=True)
    distance = Column(Float)

class Route(Base):
    __tablename__ = 'route'
    route_id = Column(Integer, primary_key=True)
    route_index = Column(Integer, primary_key=True)
    start_node = Column(String(3))
    end_node = Column(String(3))
    map_code = Column(String(2))

class RouteLink(Base):
    __tablename__ = 'route_link'
    start_node = Column(String(3), index=True, primary_key=True)
    end_node = Column(String(3), index=True, primary_key=True)
    map_code = Column(String(2), primary_key=True)
"""

@dataclass
class State:
    current_train: dict | None = None
    train_route_index: int = 0
    has_terminated: bool = False
    has_extra_details_record: bool = False
    last_route_id: int = 0

    expired_flow_ids: set[int] = field(default_factory=set)
    duplicate_trains: set[str] = field(default_factory=set)

    def reset(self):
        self.current_train = None
        self.train_route_index = 0
        self.has_terminated = False
        self.has_extra_details_record = False

def record_for_loc_entry(entry: str, _: State) -> list[Record]:
    entry_type = entry[:2]
    if entry_type == 'RL':
        end_date = parse_date_ddmmyyyy(entry[9:17])
        start_date = parse_date_ddmmyyyy(entry[17:25])
        if has_entry_expired(start_date, end_date):
            return []

        crs_code = entry[56:59]
        if len(crs_code.strip()) == 0:
            return []

        return [(LocationRecord, dict(
            uic_code = entry[2:9],
            ncl_code = entry[36:40], 
            crs_code = crs_code))]
    return []

def has_entry_expired(start: datetime.date, end: datetime.date) -> bool:
    if datetime.date.today() < start:
        return True

    # NOTE: This means there is no end date
    if end.year >= 2999:
        return False
    if datetime.date.today() > end:
        return True
    return False

def record_for_ffl_entry(entry: str, state: State) -> list[Record]:
    entry_type = entry[:2]
    if entry_type == 'RF':
        flow_id = int(entry[42:49])
        end_date = parse_date_ddmmyyyy(entry[20:28])
        start_date = parse_date_ddmmyyyy(entry[28:36])
        if has_entry_expired(start_date, end_date):
            state.expired_flow_ids.add(flow_id)
            return []

        return [(FlowRecord, dict(
            flow_id = flow_id,
            origin_code = entry[2:6],
            destination_code = entry[6:10],
            direction = entry[19],
            toc = entry[36:39],
            end_date = end_date,
            start_date = start_date))]

    if entry_type == 'RT':
        flow_id = int(entry[2:9])
        if flow_id in state.expired_flow_ids:
            return []

        return [(FareRecord, dict(
            flow_id = flow_id,
            ticket_code = entry[9:12],
            fare = int(entry[12:20])))]

    return []

def record_for_fsc_entry(entry: str, _: State) -> list[Record]:
    if len(entry) == 0 or entry[0] == '/':
        return []

    end_date = parse_date_ddmmyyyy(entry[9:17])
    start_date = parse_date_ddmmyyyy(entry[17:25])
    if has_entry_expired(start_date, end_date):
        return []

    return [(StationCluster, dict(
        cluster_id = entry[1:5],
        location_nlc = entry[5:9]))]

def record_for_tty_entry(entry: str, _: State) -> list[Record]:
    entry_type = entry[:1]
    if entry_type == 'R':
        end_date = parse_date_ddmmyyyy(entry[4:12])
        start_date = parse_date_ddmmyyyy(entry[12:20])
        if has_entry_expired(start_date, end_date):
            return []

        return [(TicketType, dict(
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
            discount_category = entry[111:113]))]

    return []

def record_for_mca_entry(entry: str, state: State) -> list[Record]:
    entry_type = entry[:2]
    if entry_type == 'BS':
        state.reset()

        train_uid = entry[3:9]
        if train_uid in state.duplicate_trains:
            return []

        days_run = entry[21:28]
        state.duplicate_trains.add(train_uid)
        state.current_train = dict(
            train_uid = train_uid,
            date_runs_from = date_to_sql(parse_date_yymmdd(entry[9:15])),
            date_runs_to = date_to_sql(parse_date_yymmdd(entry[15:21])),
            monday = days_run[0] == '1',
            tuesday = days_run[1] == '1',
            wednesday = days_run[2] == '1',
            thursday = days_run[3] == '1',
            friday = days_run[4] == '1',
            saturday = days_run[5] == '1',
            sunday = days_run[6] == '1',
            bank_holiday_running = (entry[28] == 'Y'))
        return []

    if entry_type == 'BX':
        if state.current_train is None:
            return []

        assert not state.has_extra_details_record
        state.has_extra_details_record = True
        state.current_train['rsid'] = entry[14:22]
        state.current_train['toc'] = entry[11:13]
        return []

    if entry_type == 'LO':
        if state.current_train is None or not state.has_extra_details_record:
            return []

        assert not state.has_terminated
        state.train_route_index += 1
        return [(TimetableLocation, dict(
            train_uid = state.current_train['train_uid'],
            train_route_index = state.train_route_index - 1,
            location_type = TimetableLocationType.Origin,
            location = entry[2:10].strip(),
            scheduled_departure_time = time_to_sql(parse_time(entry[10:15])),
            public_departure = parse_time(entry[15:19]),
            platform = entry[19:22].strip(),
            line = entry[22:25].strip(),
            engineering_allowance = entry[25:27].strip(),
            pathing_allowance = entry[27:29].strip(),
            activity = entry[39:41].strip(),
            performance_allowance = entry[41:43].strip()))]

    if entry_type == 'LI':
        if state.current_train is None or not state.has_extra_details_record:
            return []

        # Ignore stations we don't stop at
        scheduled_pass = entry[20:25]
        if len(scheduled_pass.strip()) != 0:
            return []

        assert not state.has_terminated
        state.train_route_index += 1
        return [(TimetableLocation, dict(
            train_uid = state.current_train['train_uid'],
            train_route_index = state.train_route_index - 1,
            location_type = TimetableLocationType.Intermediate,
            location = entry[2:10].strip(),
            scheduled_arrival_time = time_to_sql(parse_time(entry[10:15])),
            scheduled_departure_time = time_to_sql(parse_time(entry[15:20])),
            public_arrival = parse_time(entry[25:29]),
            public_departure = parse_time(entry[29:33]),
            platform = entry[33:36].strip(),
            line = entry[36:39].strip(),
            path = entry[39:42].strip(),
            activity = entry[42:54].strip(),
            engineering_allowance = entry[54:56].strip(),
            pathing_allowance = entry[56:58].strip(),
            performance_allowance = entry[58:60].strip()))]

    if entry_type == 'LT':
        if state.current_train is None or not state.has_extra_details_record:
            return []

        assert not state.has_terminated
        state.has_terminated = True

        return [
            (TrainTimetable, state.current_train),
            (TimetableLocation, dict(
                train_uid = state.current_train['train_uid'],
                train_route_index = state.train_route_index,
                location_type = TimetableLocationType.Terminating,
                location = entry[2:10].strip(),
                scheduled_arrival_time = time_to_sql(parse_time(entry[10:15])),
                public_arrival = parse_time(entry[15:19]),
                platform = entry[19:22].strip(),
                path = entry[22:25].strip(),
                activity = entry[25:37].strip()))
        ]

    if entry_type == 'TI':
        return [(TIPLOC, dict(
            tiploc_code = entry[2:9].strip(),
            crs_code = entry[53:56],
            description = entry[56:72].strip()))]

    return []

"""
def record_for_rgd_entry(entry: str, _: State) -> list[Record]:
    if len(entry) == 0 or entry[0] == '/':
        return []

    start_station, end_station, distance = entry.split(',')
    return [(StationLink, dict(
        start_station = start_station,
        end_station = end_station,
        distance = distance))]

def record_for_rgl_entry(entry: str, _: State) -> list[Record]:
    if len(entry) == 0 or entry[0] == '/':
        return []
    
    start_node, end_node, map_code = entry.split(',')
    return [(RouteLink, dict(
        start_node = start_node,
        end_node = end_node,
        map_code = map_code))]

def record_for_rgs_entry(entry: str, _: State) -> list[Record]:
    if len(entry) == 0 or entry[0] == '/':
        return []
    
    fields = entry.split(',')
    location_crs = fields[0]
    station_group_id = fields[-1]
    return [(Station, dict(
        location_crs = location_crs,
        station_group_id = station_group_id,
        node_id = station_group_id if station_group_id != '' else location_crs))]

def record_for_rgr_entry(entry: str, state: State) -> list[Record]:
    if len(entry) == 0 or entry[0] == '/':
        return []
    
    route_id = state.last_route_id
    state.last_route_id += 1

    start_node, end_node, *map_codes = entry.split(',')
    return [(
        Route, dict(
            route_id = route_id,
            route_index = i,
            start_node = start_node,
            end_node = end_node,
            map_code = map_code))
        for i, map_code in enumerate(map_codes)]
"""

def entry_parser_for_file(file: str) -> Callable[[str, State], list[Record]] | None:
    if len(file) < 3:
        return None

    parser_map = {
        'LOC': record_for_loc_entry,
        'FFL': record_for_ffl_entry,
        'FSC': record_for_fsc_entry,
        'TTY': record_for_tty_entry,
        'MCA': record_for_mca_entry,
        # 'RGD': record_for_rgd_entry,
        # 'RGL': record_for_rgl_entry,
        # 'RGS': record_for_rgs_entry,
        # 'RGR': record_for_rgr_entry,
    }
    return parser_map.get(file[-3:])

def records_in_dtd_file(chunk_queue: Queue[RecordSet],
                        entry_parser: Callable[[str, State], list[Record]],
                        path: str, file: str, 
                        progress: Progress):
    with RecordChunkGenerator(chunk_queue) as chunk_generator:
        last_progress_report = 0
        total_size_bytes = os.path.getsize(path + '/' + file)
        bytes_processed = 0

        with open(path + '/' + file, 'r') as f:
            state = State()
            line_no = 0
            for entry_line in f:
                line_no += 1

                bytes_processed += len(entry_line)
                if time.time() - last_progress_report >= 1:
                    progress.report(file, bytes_processed, total_size_bytes)
                    last_progress_report = time.time()

                for record in entry_parser(entry_line.strip(), state):
                    chunk_generator.put(record)

def records_in_dtd_file_set(executor: Executor, chunk_queue: Queue[RecordSet | None],
                            path: str, progress: Progress) -> Iterable[Future]:
    tasks: list[Future] = []
    for file in os.listdir(path):
        entry_parser = entry_parser_for_file(file)
        if entry_parser is None:
            continue

        tasks.append(executor.submit(records_in_dtd_file,
            chunk_queue, entry_parser, path, file, progress))

    return tasks

def generate_precomputed_tables(db: Session):
    from_station = aliased(TimetableLocation)
    to_station = aliased(TimetableLocation)
    select = db.query(from_station.location, to_station.location)\
        .select_from(from_station, to_station)\
        .distinct()\
        .filter(to_station.train_uid == from_station.train_uid)\
        .filter(to_station.train_route_index == from_station.train_route_index + 1)

    statement = TimetableLink.__table__\
        .insert()\
        .from_select(['from_location', 'to_location'], select.statement)
    db.execute(statement)
    db.commit()

class DTDFeed(Feed):
    def records_in_feed(self,
                        executor: Executor,
                        chunk_queue: Queue[RecordSet | None],
                        path: str,
                        progress: Progress) -> Iterable[Future]:
        zip_file_path = os.path.join(path, self.file_name())
        with ZipFile(zip_file_path, 'r') as f:
            f.extractall(path)
        os.remove(zip_file_path)

        return records_in_dtd_file_set(executor, chunk_queue, path, progress)

    def expiry_length(self) -> int:
        return 60 * 60 * 24 * 365 # 1 Year

class DTDFaresFeed(DTDFeed):
    def feed_api_url(self) -> str:
        return '2.0/fares'

    def file_name(self) -> str:
        return 'FARES.ZIP'

    def associated_tables(self) -> Iterable[type[Base]]:
        return [
            LocationRecord, StationCluster,
            FlowRecord, FareRecord, TicketType]

class DTDTimetableFeed(DTDFeed):
    def feed_api_url(self) -> str:
        return '3.0/timetable'

    def file_name(self) -> str:
        return 'TIMETABLE.ZIP'

    def preprocess_hook(self, db: Session):
        generate_precomputed_tables(db)

    def associated_tables(self) -> Iterable[type[Base]]:
        return [
            TimetableLocation, TimetableLink,
            TrainTimetable, TIPLOC]

"""
class DTDRouteingFeed(DTDFeed):
    def feed_api_url(self) -> str:
        return '2.0/routeing'

    def file_name(self) -> str:
        return 'ROUTEING.ZIP'

    def associated_tables(self) -> Iterable[type[Base]]:
        return [
            Station, StationLink,
            Route, RouteLink]
"""

Feed.register(DTDFaresFeed)
Feed.register(DTDTimetableFeed)
# Feed.register(DTDRouteingFeed)

