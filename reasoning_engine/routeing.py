from __future__ import annotations
import datetime
import copy
import math
from dataclasses import dataclass
from typing import Iterable, Iterator
from knowledge_base import TrainPath, TrainRoute, TrainRouteSegment, group
from knowledge_base.dtd import TIPLOC, date_to_sql
from knowledge_base.dtd import TimetableLocation, TrainTimetable, TimetableLink
from sqlalchemy.sql.elements import literal
from sqlalchemy.orm.session import Session

@dataclass
class JourneySegment:
    start: TimetableLocation
    end: TimetableLocation

LocationRoute = list[str]
TrainStops = list[TimetableLocation]
Journey = list[JourneySegment]

class Path:
    _stations: list[str]
    _sub_paths: list[Path]

    _locations: set[str]
    _sub_path_locations: list[set[str]]

    def __init__(self, other: Path = None):
        self._stations = copy.copy(other._stations) if other else []
        self._sub_paths = copy.copy(other._sub_paths) if other else []

        self._locations = copy.copy(other._locations) if other else set()
        self._sub_path_locations = copy.copy(other._sub_path_locations) if other else []

    def extend(self, from_location: str) -> Path:
        new_path = Path(self)
        new_path._stations.insert(0, from_location)
        new_path._locations.add(from_location)
        return new_path
    
    def merge(self, other: Path) -> Path:
        # Flatten any unneeded sub paths
        new_path = Path()
        for sub_path in [self, other]:
            new_path._sub_path_locations.append(sub_path._locations)
            new_path._sub_path_locations += sub_path._sub_path_locations
            if len(sub_path._stations) == 0:
                new_path._sub_paths += sub_path._sub_paths
            else:
                new_path._sub_paths.append(sub_path)

        return new_path

    def has_been_to(self, location: str) -> bool:
        if location in self._locations:
            return True

        return any([location in location_set 
            for location_set in self._sub_path_locations])

    def possible_paths_count(self) -> int:
        return 1 + sum([sub_path.possible_paths_count() 
            for sub_path in self._sub_paths])

    def all_locations(self) -> set[str]:
        return self._locations.union(*self._sub_path_locations)

    def routes(self) -> list[LocationRoute]:
        if len(self._sub_paths) == 0:
            return [self._stations]

        return [self._stations + sub_route
            for sub_path in self._sub_paths
            for sub_route in sub_path.routes()]

    def debug_print(self, indent: int = 0):
        print(f"{ ' ' * indent }{ ' '.join(self._stations) }")
        for sub_path in self._sub_paths:
            sub_path.debug_print(indent + 1)

def links_from_location(db: Session, from_loc: Iterable[str]):
    links = db.query(TimetableLink.from_location, TimetableLink.to_location)\
        .select_from(TimetableLink)\
        .filter(TimetableLink.from_location.in_(from_loc))\
        .all()
    return links

def search_paths(db: Session, n: int,
                 from_loc: str, to_loc: str) -> list[Path]:
    found_paths = []
    found_possible_routes_count = 0
    paths = { from_loc: Path() }
    depth = 0
    while found_possible_routes_count < n:
        next_paths = {}
        for from_location, to_location in links_from_location(db, paths.keys()):
            path = paths[from_location]
            if path.has_been_to(to_location):
                continue

            new_path = path.extend(from_location)
            if to_location in next_paths:
                new_path = new_path.merge(next_paths[to_location])
            next_paths[to_location] = new_path

        paths = next_paths
        if to_loc in paths:
            path = paths[to_loc].extend(to_loc)
            found_possible_routes_count += path.possible_paths_count()
            found_paths.append(path)
            del paths[to_loc]
        
        depth += 1
        if depth >= 400:
            break

    return found_paths

def train_stops_in_route(db: Session, route: LocationRoute,
                         date: datetime.date) -> list[TimetableLocation]:
    possible_end_locations = db.query(TimetableLocation)\
        .select_from(TimetableLocation)\
        .join(TrainTimetable, TrainTimetable.train_uid == TimetableLocation.train_uid)\
        .filter(TimetableLocation.location.in_(literal(route)))\
        .filter(date_to_sql(date) >= TrainTimetable.date_runs_from)\
        .filter(date_to_sql(date) <= TrainTimetable.date_runs_to)\
        .filter(TrainTimetable.day_to_column(date.weekday()))
    return list(possible_end_locations.all())

def train_stops_from_paths(db: Session, date: datetime.date,
                           paths: Iterable[Path]) -> list[TimetableLocation]:
    all_locations = set().union(
        *[path.all_locations() for path in paths])
    return train_stops_in_route(db, list(all_locations), date)

def sort_trains_by_uid(train_stops: TrainStops,
                       route: LocationRoute) -> dict[str, TrainStops]:
    stops_by_train_uid: dict[str, TrainStops] = {}
    index_lookup = { location: i for i, location in enumerate(route) }

    for new_stop in train_stops:
        if not new_stop.train_uid in stops_by_train_uid:
            stops_by_train_uid[new_stop.train_uid] = [new_stop]
            continue
        
        train = stops_by_train_uid[new_stop.train_uid]
        location_route_index = index_lookup[new_stop.location]

        if (new_stop.train_route_index < train[-1].train_route_index
            and location_route_index > index_lookup[train[-1].location]):
            train.append(new_stop)
            continue

        last_stop_route_index = math.inf
        for i, next_stop in enumerate(train):
            if (new_stop.train_route_index > next_stop.train_route_index
                and new_stop.train_route_index < last_stop_route_index
                and location_route_index < index_lookup[next_stop.location]):
                train.insert(i, new_stop)
                break
            last_stop_route_index = next_stop.train_route_index
    return {
        train_uid: stops
        for train_uid, stops in stops_by_train_uid.items()
        if len(stops) > 1 }

def search_train_route(start: str,
                       train_paths: Iterable[TrainPath],
                       route: LocationRoute,
                       train_route: TrainRoute = [],
                       ) -> TrainRoute | None:
    if len(train_route) > 3:
        return None

    trains = [train for train in train_paths if start in train]
    for train in trains:
        end_location = route[0]
        if end_location in train:
            return train_route + [TrainRouteSegment(train, start, end_location)]

        for stop in train:
            if route.index(stop) >= route.index(start):
                continue

            result = search_train_route(stop, train_paths, route,
                train_route + [TrainRouteSegment(train, start, stop)])
            if not result is None:
                return result
    return None

def find_journeys(trains_by_paths: dict[TrainPath, list[TrainStops]],
                  train_route: TrainRoute) -> list[Journey]:
    start_trains = trains_by_paths[train_route[0].path]
    journeys: list[Journey] = []
    for start_train in start_trains:
        first_start = start_train[-1]
        first_stop = next(filter(lambda x: x.location == train_route[0].stop_location, start_train))
        journey: Journey = [JourneySegment(first_start, first_stop)]
        for segment in train_route[1:]:
            trains = [(stop, train)
                for train in trains_by_paths[segment.path]
                for stop in train
                    if (stop.location == journey[-1].end.location and
                        stop.scheduled_departure_time > journey[-1].end.scheduled_arrival_time)]
            if len(trains) == 0:
                break

            start, train = min(trains, key=lambda x: x[0].scheduled_departure_time)
            stop = next(filter(lambda x: x.location == segment.stop_location, train))
            journey.append(JourneySegment(start, stop))
        else:
            journeys.append(journey)
    return journeys

def find_journeys_for_route(route: LocationRoute,
                            all_train_stops: list[TimetableLocation]
                            ) -> tuple[TrainRoute, list[Journey]] | None:
    train_stops = [stop for stop in all_train_stops if stop.location in route]
    stops_by_train_uid = sort_trains_by_uid(train_stops, route)
    trains_by_paths = group(
        stops_by_train_uid.values(),
        lambda x: tuple([str(stop.location) for stop in x]))
    train_paths = trains_by_paths.keys()

    start_location = route[-1]
    train_route = search_train_route(start_location, train_paths, route)
    if train_route is None:
        return None
    
    return train_route, find_journeys(trains_by_paths, train_route)

def find_journeys_for_paths(db: Session, date: datetime.date,
                            paths: Iterable[Path]) -> Iterator[tuple[TrainRoute, list[Journey]]]:
    all_train_stops = train_stops_from_paths(db, date, paths)
    for route in [route for path in paths for route in path.routes()]:
        result = find_journeys_for_route(route, all_train_stops)
        if not result is None:
            yield result

def crs_route_to_tiploc_route(db: Session, crs_route: LocationRoute) -> LocationRoute:
    crs_to_tiploc_map = {
        crs: tiploc
        for crs, tiploc in db\
            .query(TIPLOC.crs_code, TIPLOC.tiploc_code)\
            .filter(TIPLOC.crs_code.in_(crs_route))\
            .all() }
    return [crs_to_tiploc_map[crs] for crs in crs_route]

def find_journeys_from_crs(db: Session, from_crs: str, to_crs: str,
                           date: datetime.date) -> Iterator[Journey]:
    from_loc, to_loc = crs_route_to_tiploc_route(db, [from_crs, to_crs])
    found_paths = search_paths(db, 4, from_loc, to_loc)

    train_routes = find_journeys_for_paths(db, date, found_paths)
    for _, journeys in train_routes:
        yield from journeys

