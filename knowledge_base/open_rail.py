import datetime
from operator import and_
from knowledge_base.dtd import FareRecord, FlowRecord, LocationRecord, TicketType
from knowledge_base.dtd import TIPLOC, TimetableLocation, TrainTimetable
from sqlalchemy.orm.util import aliased
from sqlalchemy.orm.session import Session

def station_timetable(db: Session, from_location: str, to_location: str, 
                      date: datetime.date) -> list[TimetableLocation]:
    current_day = datetime.datetime.now().weekday()
    next_station = aliased(TimetableLocation)
    from_tiploc = aliased(TIPLOC)
    to_tiploc = aliased(TIPLOC)

    # NOTE: Type checking breaks here for some reason
    result = db.query(TimetableLocation)\
        .join(TrainTimetable, TrainTimetable.train_uid == TimetableLocation.train_uid)\
        .outerjoin(next_station,and_(\
            next_station.train_uid == TimetableLocation.train_uid,\
            next_station.train_route_index == TimetableLocation.train_route_index + 1))\
        .join(from_tiploc, from_tiploc.tiploc_code == TimetableLocation.location)\
        .join(to_tiploc, to_tiploc.tiploc_code == next_station.location, isouter=True)\
        .filter(date >= TrainTimetable.date_runs_from)\
        .filter(date <= TrainTimetable.date_runs_to)\
        .filter(from_tiploc.crs_code == from_location)\
        .filter(to_tiploc.crs_code == to_location)\
        .add_columns(TrainTimetable.days_run)\
        .all()

    return [s for (s, days_run) in result if days_run[current_day] == '1']

def ticket_prices(db: Session, from_location: str, to_location: str) -> list[tuple[int, TicketType]]:
    origin = aliased(LocationRecord)
    destination = aliased(LocationRecord)

    # NOTE: Type checking breaks here for some reason
    result = db.query(FareRecord)\
        .join(FlowRecord, FlowRecord.flow_id == FareRecord.flow_id)\
        .join(origin, origin.ncl_code == FlowRecord.origin_code)\
        .join(destination, destination.ncl_code == FlowRecord.destination_code)\
        .join(TicketType, TicketType.ticket_code == FareRecord.ticket_code)\
        .filter(origin.crs_code == from_location)\
        .filter(destination.crs_code == to_location)\
        .add_entity(TicketType)\
        .all()

    return [(fare.fare, ticket) for fare, ticket in result]

