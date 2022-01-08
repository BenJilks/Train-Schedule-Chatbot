import datetime
from dataclasses import dataclass
from itertools import accumulate, groupby
from knowledge_base.dtd import FareRecord, FlowRecord, LocationRecord, TicketType
from knowledge_base.dtd import TIPLOC, TimetableLocation, TrainTimetable
from sqlalchemy.orm.util import aliased
from sqlalchemy.orm.session import Session

@dataclass
class Route:
    start: TimetableLocation
    change_over: list[tuple[TimetableLocation, TimetableLocation]]
    end: TimetableLocation

    def __init__(self, *stations):
        assert len(stations) >= 2
        self.start = stations[0]
        self.end = stations[-1]

        changes = stations[1:-1]
        assert len(changes) % 2 == 0
        self.change_over = list(zip(changes[::2], changes[1::2]))
    
    def __repr__(self) -> str:
        return (
            f"{ self.start.public_departure } -> " +
            ''.join([
                f"{ stop.public_arrival } { stop.location :^10} { join.public_departure } -> "
                for stop, join in self.change_over]) +
            f"{ self.end.public_arrival }")

def possible_change_over_locations_query(db: Session, from_location: str, to_location: str):
    previous_station = aliased(TimetableLocation)
    stations_leading_to_end = db.query(previous_station)\
        .select_from(TimetableLocation)\
        .join(previous_station, previous_station.train_uid == TimetableLocation.train_uid)\
        .filter(TimetableLocation.location == to_location)\
        .filter(previous_station.train_route_index < TimetableLocation.train_route_index)\
        .group_by(previous_station.location)\
        .subquery()

    stop_station = aliased(TimetableLocation)
    join_station = aliased(previous_station, stations_leading_to_end)
    result = db.query(stop_station.location)\
        .select_from(TimetableLocation)\
        .join(stop_station, stop_station.train_uid == TimetableLocation.train_uid)\
        .join(join_station, join_station.location == stop_station.location)\
        .filter(TimetableLocation.location == from_location)\
        .filter(stop_station.train_route_index > TimetableLocation.train_route_index)\
        .group_by(stop_station.location)\
        .subquery()
    return result

def trains_for_route(db: Session, from_location, to_location, date: datetime.date):
    start_station = aliased(TimetableLocation)
    target_station = aliased(TimetableLocation)
    current_day = datetime.datetime.now().weekday()
    day_pattern = ''.join(['1' if day == current_day else '_' for day in range(7)])

    result = db.query(start_station, target_station)\
        .select_from(start_station)\
        .join(TrainTimetable, TrainTimetable.train_uid == start_station.train_uid)\
        .join(target_station, target_station.train_uid == start_station.train_uid)\
        .filter(date >= TrainTimetable.date_runs_from)\
        .filter(date <= TrainTimetable.date_runs_to)\
        .filter(TrainTimetable.days_run.like(day_pattern))\
        .filter(target_station.train_route_index > start_station.train_route_index)\
        .filter(start_station.location == from_location)\
        .filter(target_station.location == to_location)
    return result

def group(it, key):
    return { k: list(g) for k, g in groupby(sorted(it, key=key), key=key) }

def find_single_train_routes(db: Session, from_code: str, to_code: str, 
                             date: datetime.date) -> list[Route]:
    trains = trains_for_route(db, from_code, to_code, date).all()
    return [Route(*x) for x in trains]

def find_two_train_routes(db: Session, from_code: str, to_code: str, 
                          date: datetime.date) -> list[Route]:
    possible_locations = possible_change_over_locations_query(
        db, from_code, to_code)

    change_over_location = possible_locations.c.location
    start_to_stop = trains_for_route(db, from_code, change_over_location, date).all()
    stop_to_end = trains_for_route(db, change_over_location, to_code, date).all()

    # Match each starting route to an ending one
    trains_from_location = group(stop_to_end, lambda x: x[0].location)
    return [Route(*route)
        for route in [
            (start, stop,
            *min(
                [(join, end)
                    for join, end in trains_from_location[stop.location]
                    if join.scheduled_departure_time > stop.scheduled_arrival_time],
                default = [],
                key = lambda x: x[0].scheduled_departure_time))
            for start, stop in start_to_stop
            if stop.location in trains_from_location]
        if len(route) == 4]

def find_routes(db: Session, from_location: str, to_location: str, 
                date: datetime.date) -> list[Route]:
    from_tiploc = db.query(TIPLOC).filter(TIPLOC.crs_code == from_location).first().tiploc_code
    to_tiploc = db.query(TIPLOC).filter(TIPLOC.crs_code == to_location).first().tiploc_code
    
    routes = (
        find_single_train_routes(db, from_tiploc, to_tiploc, date) +
        find_two_train_routes(db, from_tiploc, to_tiploc, date))
    
    # Pick the fastest route for each time
    routes = sorted(
        [min(route, key=lambda x: x.end.scheduled_arrival_time)
            for route in group(routes, lambda x: x.start.scheduled_departure_time).values()],
        key=lambda route: route.start.scheduled_departure_time)

    # Don't show any routes that start earlier and arrive later
    return [route
        for smallest_time, route in zip(
            accumulate(
                [route.end.scheduled_arrival_time for route in routes], max,
                initial=datetime.time(0, 0)),
            routes)
        if route.end.scheduled_arrival_time > smallest_time]

def ticket_prices(db: Session, from_location: str, to_location: str) -> list[tuple[int, TicketType]]:
    origin = aliased(LocationRecord)
    destination = aliased(LocationRecord)
    result = db.query(FareRecord, TicketType)\
        .select_from(FareRecord)\
        .join(FlowRecord, FlowRecord.flow_id == FareRecord.flow_id)\
        .join(origin, origin.ncl_code == FlowRecord.origin_code)\
        .join(destination, destination.ncl_code == FlowRecord.destination_code)\
        .join(TicketType, TicketType.ticket_code == FareRecord.ticket_code)\
        .filter(origin.crs_code == from_location)\
        .filter(destination.crs_code == to_location)\
        .all()

    return [(fare.fare, ticket) for fare, ticket in result]

