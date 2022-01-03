from knowledge_base.dtd import FareRecord, FlowRecord, LocationRecord, TicketType
from sqlalchemy.orm.util import aliased
from sqlalchemy.orm.session import Session

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
        .add_entity(TicketType)

    return [(fare.fare, ticket) for _, fare, ticket in result.all()]

