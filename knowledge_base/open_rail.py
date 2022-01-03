from dtd import FareRecord, FlowRecord, LocationRecord
from sqlalchemy.orm.util import aliased
from sqlalchemy.orm.session import Session

def ticket_prices(db: Session, from_location: str, to_location: str) -> list[int]:
    origin = aliased(LocationRecord)
    destination = aliased(LocationRecord)
    fare = aliased(FareRecord)

    # NOTE: Type checking breaks here for some reason
    result = db.query(FlowRecord)\
        .join(origin, origin.ncl_code == FlowRecord.origin_code)\
        .join(destination, destination.ncl_code == FlowRecord.destination_code)\
        .join(fare, fare.flow_id == FlowRecord.flow_id)\
        .filter(origin.crs_code == from_location)\
        .filter(destination.crs_code == to_location)\
        .add_entity(fare)

    return [fare.fare for _, fare in result.all()]

