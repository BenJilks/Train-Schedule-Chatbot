import datetime
from knowledge_base.dtd import FareRecord, FlowRecord, LocationRecord, TicketType
from knowledge_base.dtd import TIPLOC, TimetableLocation, TrainTimetable
from sqlalchemy.orm.util import aliased
from sqlalchemy.orm.session import Session

def station_timetable(db: Session, location: str, date: datetime.date) -> list[TimetableLocation]:
    current_day = datetime.datetime.now().weekday()

    # NOTE: Type checking breaks here for some reason
    result = db.query(TimetableLocation)\
        .join(TIPLOC, TIPLOC.tiploc_code == TimetableLocation.location)\
        .join(TrainTimetable, TrainTimetable.train_uid == TimetableLocation.train_uid)\
        .filter(TIPLOC.crs_code == location)\
        .filter(date >= TrainTimetable.date_runs_from)\
        .filter(date <= TrainTimetable.date_runs_to)\
        .add_columns(TrainTimetable.days_run)\
        .all()
    return [x for (x, days_run) in result if days_run[current_day] == '1']

def ticket_prices(db: Session, from_location: str, to_location: str) -> list[tuple[int, TicketType]]:
    origin = aliased(LocationRecord)
    destination = aliased(LocationRecord)

    # NOTE: Type checking breaks here for some reason
    result = db.query(FlowRecord)\
        .join(origin, origin.ncl_code == FlowRecord.origin_code)\
        .join(destination, destination.ncl_code == FlowRecord.destination_code)\
        .join(FareRecord, FareRecord.flow_id == FlowRecord.flow_id)\
        .join(TicketType, TicketType.ticket_code == FareRecord.ticket_code)\
        .filter(origin.crs_code == from_location)\
        .filter(destination.crs_code == to_location)\
        .add_entity(FareRecord)\
        .add_entity(TicketType)\
        .all()

    return [(fare.fare, ticket) for _, fare, ticket in result]

