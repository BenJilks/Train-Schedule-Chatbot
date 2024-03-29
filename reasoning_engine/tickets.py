from sqlalchemy.orm.session import Session
from sqlalchemy.sql.elements import literal
from sqlalchemy.sql import func
from knowledge_base import group
from knowledge_base.dtd import FareRecord, FlowRecord, TicketType
from knowledge_base.dtd import LocationRecord, StationCluster

def ncl_for_location_crs(db: Session, *crs: str) -> list[tuple[str, list[str]]]:
    result = (
        db.query(
            LocationRecord.ncl_code,
            func.ifnull(StationCluster.cluster_id, LocationRecord.ncl_code))\
        .select_from(LocationRecord)\
        .outerjoin(StationCluster, StationCluster.location_nlc == LocationRecord.ncl_code)\
        .filter(LocationRecord.crs_code.in_(literal(crs)))\
        .all())

    return [
        (ncl, [cluster_id for _, cluster_id in clusters])
        for ncl, clusters in group(result, lambda x: x[0]).items()]

def internal_tickets(db: Session, from_ncl: str, to_clusters: list[str]):
    return db.query(FareRecord, TicketType)\
        .select_from(FareRecord)\
        .join(FlowRecord, FlowRecord.flow_id == FareRecord.flow_id)\
        .join(TicketType, TicketType.ticket_code == FareRecord.ticket_code)\
        .join(StationCluster, StationCluster.location_nlc == FlowRecord.destination_code)\
        .outerjoin(LocationRecord, LocationRecord.ncl_code == FlowRecord.destination_code)\
        .filter(LocationRecord.crs_code == None)\
        .filter(StationCluster.cluster_id.in_(to_clusters))\
        .filter(FlowRecord.origin_code == from_ncl)\
        .all()

def internal_tickets_reversed(db: Session, from_ncl: str, to_clusters: list[str]):
    return db.query(FareRecord, TicketType)\
        .select_from(FareRecord)\
        .join(FlowRecord, FlowRecord.flow_id == FareRecord.flow_id)\
        .join(TicketType, TicketType.ticket_code == FareRecord.ticket_code)\
        .join(StationCluster, StationCluster.location_nlc == FlowRecord.origin_code)\
        .outerjoin(LocationRecord, LocationRecord.ncl_code == FlowRecord.origin_code)\
        .filter(FlowRecord.direction == 'R')\
        .filter(LocationRecord.crs_code == None)\
        .filter(StationCluster.cluster_id.in_(to_clusters))\
        .filter(FlowRecord.destination_code == from_ncl)\
        .all()

def ticket_prices(db: Session, from_location: str, to_location: str) -> list[tuple[int, TicketType]]:
    (from_ncl, from_clusters), (_, to_clusters) = ncl_for_location_crs(db, from_location, to_location)
    result = db.query(FareRecord, TicketType)\
        .select_from(FareRecord)\
        .join(FlowRecord, FlowRecord.flow_id == FareRecord.flow_id)\
        .join(TicketType, TicketType.ticket_code == FareRecord.ticket_code)\
        .filter(FlowRecord.origin_code.in_(literal(from_clusters)))\
        .filter(FlowRecord.destination_code.in_(literal(to_clusters)))\
        .all()

    if len(result) != 0:
        return [(fare.fare, ticket) for fare, ticket in result]

    internal = (
        internal_tickets(db, from_ncl, to_clusters) +
        internal_tickets_reversed(db, from_ncl, to_clusters))
    return [(fare.fare, ticket) for fare, ticket in internal]

