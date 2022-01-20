import os
import time
from datetime import datetime
from queue import Queue
from typing import Iterable
from concurrent.futures import Executor, Future
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import Boolean, DateTime, String, Text
from knowledge_base.feeds import Base, Feed, RecordChunkGenerator, RecordSet
from knowledge_base.progress import Progress
import xml.etree.ElementTree as ET

NAMESPACES = {
    'i': 'http://nationalrail.co.uk/xml/incident',
    's': 'http://nationalrail.co.uk/xml/station'
}

class Incident(Base):
    __tablename__ = 'incidents'
    incident_number = Column(String(32), primary_key=True)
    creation_time = Column(DateTime)
    planned = Column(Boolean)
    summery = Column(Text)
    description = Column(Text)
    cleared_incident = Column(Boolean)
    route_affected = Column(Text)

class IncidentAffectedOperators(Base):
    __tablename__ = 'incident_affected_operators'
    incident_number = Column(String(32), primary_key=True)
    operator_toc = Column(String(2), primary_key=True)
    operator_name = Column(Text)

class Station(Base):
    __tablename__ = 'station'
    crs_code = Column(String(3), primary_key=True)
    name = Column(Text)

def parse_datetime(datetime_str: str) -> datetime:
    return datetime.strptime(datetime_str.split('.')[0], '%Y-%m-%dT%H:%M:%S')

def records_for_incidents(incidents: list[ET.Element], chunk_queue: Queue[RecordSet],
                          progress: Progress):
    incident_count = len(incidents)
    incidents_done = 0
    time_since_last_progress_report = time.time()
    progress.report('Incidents', 0, incident_count)

    with RecordChunkGenerator(chunk_queue) as chunk_generator:
        for incident in incidents:
            incident_number = incident.findtext('i:IncidentNumber', namespaces=NAMESPACES)
            if incident_number is None:
                continue

            affects = incident.find('i:Affects', namespaces=NAMESPACES)
            assert not affects is None

            chunk_generator.put((Incident, dict(
                incident_number = incident_number,
                creation_time = parse_datetime(incident.findtext('i:CreationTime', '', namespaces=NAMESPACES)),
                planned = (incident.findtext('i:Planned', namespaces=NAMESPACES) == 'true'),
                summery = incident.findtext('i:Summary', namespaces=NAMESPACES),
                description = incident.findtext('i:Description', namespaces=NAMESPACES),
                cleared_incident = (incident.findtext('i:ClearedIncident', namespaces=NAMESPACES) == 'true'),
                route_affected = affects.findtext('i:RoutesAffected', namespaces=NAMESPACES))))
            
            operators = affects.find('i:Operators', NAMESPACES) 
            assert not operators is None

            for operator in operators:
                chunk_generator.put((IncidentAffectedOperators, dict(
                    incident_number = incident_number,
                    operator_toc = operator.findtext('i:OperatorRef', namespaces=NAMESPACES),
                    operator_name = operator.findtext('i:OperatorName', namespaces=NAMESPACES))))

            incidents_done += 1
            if time.time() - time_since_last_progress_report >= 1:
                progress.report('Incidents', incidents_done, incident_count)
                time_since_last_progress_report = time.time()
    progress.report('Incidents', incident_count, incident_count)

def records_for_stations(stations: list[ET.Element], chunk_queue: Queue[RecordSet],
                         progress: Progress):
    station_count = len(stations)
    stations_done = 0
    time_since_last_progress_report = time.time()
    progress.report('Stations', 0, station_count)

    with RecordChunkGenerator(chunk_queue) as chunk_generator:
        for station in stations:
            chunk_generator.put((Station, dict(
                crs_code = station.findtext('s:CrsCode', namespaces=NAMESPACES),
                name = station.findtext('s:Name', namespaces=NAMESPACES))))

            stations_done += 1
            if time.time() - time_since_last_progress_report >= 1:
                progress.report('Stations', stations_done, station_count)
                time_since_last_progress_report = time.time()
    progress.report('Stations', station_count, station_count)

class KBIncidents(Feed):
    def associated_tables(self) -> Iterable[type[Base]]:
        return [Incident, IncidentAffectedOperators]

    def file_name(self) -> str:
        return 'INCIDENTS.XML'

    def expiry_length(self) -> int:
        return 60 * 5 # 5 Minutes

    def feed_api_url(self) -> str:
        return '5.0/incidents'

    def records_in_feed(self,
                        executor: Executor,
                        chunk_queue: Queue[RecordSet | None],
                        path: str,
                        progress: Progress) -> Iterable[Future]:
        tree = ET.parse(os.path.join(path, self.file_name()))
        incidents = [incident for incident in tree.getroot()]

        return [executor.submit(records_for_incidents,
            incidents, chunk_queue, progress)]

class KBStations(Feed):
    def associated_tables(self) -> Iterable[type[Base]]:
        return [Station]

    def file_name(self) -> str:
        return 'STATIONS.XML'

    def expiry_length(self) -> int:
        return 60 * 60 * 24 # 1 Day

    def feed_api_url(self) -> str:
        return '4.0/stations'

    def records_in_feed(self,
                        executor: Executor,
                        chunk_queue: Queue[RecordSet | None],
                        path: str,
                        progress: Progress) -> Iterable[Future]:
        tree = ET.parse(os.path.join(path, self.file_name()))
        stations = [station for station in tree.getroot()]

        return [executor.submit(records_for_stations,
            stations, chunk_queue, progress)]

Feed.register(KBIncidents)
Feed.register(KBStations)

