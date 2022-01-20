import regex
from typing import Iterable
from sqlalchemy.orm.session import Session
from knowledge_base.dtd import TIPLOC
from knowledge_base.kb import Incident, IncidentAffectedOperators, Station
from reasoning_engine.routeing import Journey, RouteAndJourneys

def incidents_for_toc(db: Session, toc: str) -> Iterable[Incident]:
    return db.query(Incident)\
        .select_from(IncidentAffectedOperators)\
        .distinct()\
        .join(Incident, Incident.incident_number == IncidentAffectedOperators.incident_number)\
        .filter(IncidentAffectedOperators.operator_toc == toc)\
        .all()

def tiploc_to_name(db: Session, tiploc: str) -> str:
    return db.query(Station.name)\
        .select_from(Station, TIPLOC)\
        .filter(Station.crs_code == TIPLOC.crs_code)\
        .filter(TIPLOC.tiploc_code == tiploc)\
        .first()[0]

def generate_names_to_location_map(db: Session) -> dict[str, str]:
    return {
        name: tiploc
        for name, tiploc in db.query(Station.name, TIPLOC.tiploc_code)\
            .select_from(Station, TIPLOC)\
            .filter(Station.crs_code == TIPLOC.crs_code)\
            .all()}

def parse_incident_routes(name_location_map: dict[str, str],
                          route_text: str) -> tuple[list[str], list[str]] | None:
    and_index = route_text.find('and')
    if and_index == -1:
        return None

    also_index = route_text.find('also')
    if also_index != -1:
        route_text = route_text[:also_index]

    location_indexes = [
        (name, route_text.find(name))
        for name in name_location_map.keys()
        if name in route_text]

    from_locations = [
        name_location_map[name]
        for name, index in location_indexes
        if index < and_index]
    to_locations = [
        name_location_map[name]
        for name, index in location_indexes
        if index > and_index]
    return from_locations, to_locations

def strip_html(html: str) -> str:
    stripped = html
    while True:
        new_stripped = regex.sub('<[^>]*>', '', html)
        if new_stripped == stripped:
            return stripped
        stripped = new_stripped

def find_delays(db: Session,
                routes_and_journeys: Iterable[RouteAndJourneys]
                ) -> list[tuple[Journey, Incident]]:
    name_location_map = generate_names_to_location_map(db)
    possible_incidents: set[tuple[Journey, Incident]] = set()

    for route, journeys in routes_and_journeys:
        for journey in journeys:
            for route_segment, journey_segment in zip(route, journey):
                toc = journey_segment.train.toc
                incidents = incidents_for_toc(db, toc)
                for incident in incidents:
                    result = parse_incident_routes(name_location_map, incident.route_affected)
                    if result is None:
                        continue

                    from_locations, to_locations = result
                    if not any([location in route_segment.path for location in from_locations]):
                        continue
                    if not any([location in route_segment.path for location in to_locations]):
                        continue
                    possible_incidents.add((tuple(journey), incident))
    return list(possible_incidents)

