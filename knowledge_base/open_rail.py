from __future__ import annotations

import datetime
from dataclasses import dataclass
from itertools import accumulate, groupby
from knowledge_base.dtd import FareRecord, FlowRecord, LocationRecord, TicketType
from knowledge_base.dtd import TIPLOC, TimetableLocation, TrainTimetable
from sqlalchemy.sql.elements import literal
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

    def join(self, other: Route | None):
        if other is None:
            return None

        new_route = Route(self.start, *[y for x in self.change_over for y in x], self.end)
        new_route.change_over.append((self.end, other.start))
        new_route.change_over += other.change_over
        new_route.end = other.end
        return new_route
    
    def __repr__(self) -> str:
        return (
            f"{ self.start.public_departure } -> " +
            ''.join([
                f"{ stop.public_arrival } { stop.location :^10} { join.public_departure } -> "
                for stop, join in self.change_over]) +
            f"{ self.end.public_arrival }")

def possible_two_way_change_over_locations(db: Session, from_location: str, to_location: str):
    start = aliased(TimetableLocation)
    stop_a = aliased(TimetableLocation)
    a_to_b = literal([x[0] for x in db.query(stop_a.location)\
        .select_from(start, stop_a)\
        .distinct()\
        .filter(start.location == from_location)\
        .filter(stop_a.train_uid == start.train_uid)\
        .filter(stop_a.train_route_index > start.train_route_index)\
        .all()])

    join_b = aliased(TimetableLocation)
    end = aliased(TimetableLocation)
    b_to_c = db.query(join_b.location)\
        .select_from(join_b, end)\
        .distinct()\
        .filter(join_b.location.in_(a_to_b))\
        .filter(end.location == to_location)\
        .filter(join_b.train_uid == end.train_uid)\
        .filter(join_b.train_route_index < end.train_route_index)
    return [x[0] for x in b_to_c.all()]

def possible_three_way_change_over_locations(db: Session, from_location: str, to_location: str):
    start = aliased(TimetableLocation)
    stop_a = aliased(TimetableLocation)
    a_to_b = literal([x[0] for x in db.query(stop_a.location)\
        .select_from(start, stop_a)\
        .distinct()\
        .filter(stop_a.train_uid == start.train_uid)\
        .filter(start.location == from_location)\
        .filter(stop_a.train_route_index > start.train_route_index)\
        .all()])

    join_c = aliased(TimetableLocation)
    end = aliased(TimetableLocation)
    c_to_d = literal([x[0] for x in db.query(join_c.location)\
        .select_from(join_c, end)\
        .distinct()\
        .filter(end.train_uid == join_c.train_uid)\
        .filter(end.location == to_location)\
        .filter(end.train_route_index > join_c.train_route_index)\
        .all()])

    join_b = aliased(TimetableLocation)
    stop_b = aliased(TimetableLocation)
    b_to_c = db.query(join_b.location, stop_b.location)\
        .select_from(join_b, stop_b)\
        .distinct()\
        .filter(join_b.location.in_(a_to_b))\
        .filter(stop_b.location.in_(c_to_d))\
        .filter(join_b.train_uid == stop_b.train_uid)\
        .filter(join_b.train_route_index < stop_b.train_route_index)
    return b_to_c.all()

def find_single_train_routes(db: Session, from_location: str | list[str], to_location: str | list[str], 
                             date: datetime.date) -> list[Route]:
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

    if isinstance(from_location, str):
        result = result.filter(start_station.location == literal(from_location))
    else:
        result = result.filter(start_station.location.in_(literal(from_location)))

    if isinstance(to_location, str):
        result = result.filter(target_station.location == literal(to_location))
    else:
        result = result.filter(target_station.location.in_(literal(to_location)))
    return [Route(*x) for x in result]

def group(it, key):
    return { k: list(g) for k, g in groupby(sorted(it, key=key), key=key) }

def link_train_routes(a_to_b_routes: list[Route], b_to_c_routes: list[Route]) -> list[Route]:
    # Match each starting route to an ending one
    routes_from_location: dict[str, list[Route]] = group(b_to_c_routes, lambda b_to_c: b_to_c.start.location)
    return [route
        for route in [
            a_to_b.join(min(
                [b_to_c
                    for b_to_c in routes_from_location[a_to_b.end.location]
                    if b_to_c.start.scheduled_departure_time > a_to_b.end.scheduled_arrival_time],
                default = None,
                key = lambda x: x.start.scheduled_departure_time))
            for a_to_b in a_to_b_routes
            if a_to_b.end.location in routes_from_location]
        if not route is None]

def find_two_train_routes(db: Session, from_location: str, to_location: str, date: datetime.date):
    possible_b_locations = possible_two_way_change_over_locations(
        db, from_location, to_location)

    a_to_b = find_single_train_routes(db, from_location, possible_b_locations, date)
    b_to_c = find_single_train_routes(db, possible_b_locations, to_location, date)
    a_to_d = link_train_routes(a_to_b, b_to_c)
    return a_to_d

def find_three_train_routes(db: Session, from_location: str, to_location: str, date: datetime.date):
    possible_locations = possible_three_way_change_over_locations(
        db, from_location, to_location)

    possible_b_locations = list(set([b for b, _ in possible_locations]))
    possible_c_locations = list(set([c for _, c in possible_locations]))

    a_to_b = find_single_train_routes(db, from_location, possible_b_locations, date)
    b_to_c = find_single_train_routes(db, possible_c_locations, possible_b_locations, date)
    c_to_d = find_single_train_routes(db, possible_b_locations, to_location, date)
    a_to_d = link_train_routes(link_train_routes(a_to_b, b_to_c), c_to_d)
    return a_to_d

def find_routes(db: Session, from_location: str, to_location: str, 
                date: datetime.date) -> list[Route]:
    from_tiploc = db.query(TIPLOC).filter(TIPLOC.crs_code == from_location).first().tiploc_code
    to_tiploc = db.query(TIPLOC).filter(TIPLOC.crs_code == to_location).first().tiploc_code
    
    routes = (
        find_single_train_routes(db, from_tiploc, to_tiploc, date) +
        find_two_train_routes(db, from_tiploc, to_tiploc, date))

    # Only look for three train routes if there's not enough, as this is expensive
    if len(routes) < 10:
        routes += find_three_train_routes(db, from_tiploc, to_tiploc, date)
    
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

