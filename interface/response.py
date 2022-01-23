import datetime
from dataclasses import dataclass
from enum import Enum, auto
from typing import Iterable
from interface.bot import ConversationState
from knowledge_base.dtd import TicketType
from knowledge_base.kb import Incident
from reasoning_engine.routeing import Journey

pending_response_template = (
"""
Gathering route information from {from_loc} to {to_loc}. On {date} from {time}
""")

journey_template = (
"""
The latest train will be leaving {from_loc} at {departure_time}, it will arrive at {to_loc} at {arrival_time}.
Single ticket: {single_ticket_price}
Return ticket: {return_ticket_price}
{link}
""")

alt_journey_template = (
"""
An alternative journey departs at {alt_departure_time} and arrives at {alt_arrival_time}.
"""
)

delay_template = (
"""
The train in expected to be {delay}.
""")

incidents_template = (
"""
Incidents that may effect your journey:
{incidents}
"""
)

@dataclass
class UserLocation:
    crs: str
    name: str

@dataclass
class UserInfo:
    from_loc: UserLocation
    to_loc: UserLocation
    journey: Journey
    alt_journey: Journey | None
    incidents: list[Incident]
    possible_delay: int | None
    tickets: list[tuple[int, TicketType]]

class TicketFor(Enum):
    Adult = auto()
    Child = auto()

@dataclass
class RoutePlanningState(ConversationState):
    from_loc: UserLocation | None = None
    to_loc: UserLocation | None = None
    date: datetime.date = datetime.date.today()
    time: datetime.time = datetime.datetime.now().time()
    request_incidents: bool = True
    ticket_for: TicketFor = TicketFor.Adult
    user_info: UserInfo | None = None

def format_pending_response(state: RoutePlanningState) -> str:
    assert not state.from_loc is None
    assert not state.to_loc is None
    return pending_response_template.format(
        from_loc = state.from_loc.name,
        to_loc = state.to_loc.name,
        date = state.date,
        time = state.time)

def format_price(price: int | None) -> str:
    if price is None:
        return 'None'
    pounds = price // 100
    pence = price % 100
    return f'£{pounds:02}.{pence:02}'

def ticket_matches_plan(ticket: TicketType, state: RoutePlanningState):
    if ticket.tkt_group != 'S':
        return False
    if ticket.discount_category != '01':
        return False

    if state.ticket_for == TicketFor.Adult and ticket.max_adults == 0:
        return False
    if state.ticket_for == TicketFor.Child and ticket.max_children == 0:
        return False
    return True

def get_link(state: RoutePlanningState):
    assert not state.from_loc is None
    assert not state.to_loc is None

    from_crs = state.from_loc.crs
    to_crs = state.to_loc.crs
    date = state.date.strftime('%d%m%y')
    time = f"{ state.time.strftime('%H') }00"
    query = f'{ from_crs }/{ to_crs }/{ date }/{ time }/dep'
    return f'https://ojp.nationalrail.co.uk/service/timesandfares/{ query }'

def format_journey_response(state: RoutePlanningState,
                            journey: Journey,
                            tickets: list[tuple[int, TicketType]]) -> str:
    assert not state.from_loc is None
    assert not state.to_loc is None
    start_station = journey[0].start
    end_station = journey[-1].end

    tickets.sort(key = lambda x: x[0])
    singles = [(fare, ticket) for fare, ticket in tickets
        if ticket.tkt_type == 'S' and ticket_matches_plan(ticket, state)]

    returns = [(fare, ticket) for fare, ticket in tickets
        if ticket.tkt_type == 'R' and ticket_matches_plan(ticket, state)]

    cheapest_single = singles[0][0] if len(singles) != 0 else None
    cheapest_return = returns[0][0] if len(returns) != 0 else None
    return journey_template.format(
        from_loc = state.from_loc.name,
        to_loc = state.to_loc.name,
        departure_time = start_station.public_departure,
        arrival_time = end_station.public_arrival,
        single_ticket_price = format_price(cheapest_single),
        return_ticket_price = format_price(cheapest_return),
        link = get_link(state))

def format_incidents_response(incidents: Iterable[Incident]) -> str:
    incidents_str = '\n'.join([
        incident.summery for incident in incidents])
    return incidents_template.format(
        incidents = incidents_str)

def format_possible_delay(possible_delay: int | None) -> str:
    if possible_delay is None:
        return 'on time'
    return f'up to { possible_delay } minutes late'

def format_delays_response(possible_delay: int | None,
                           alt_journey: Journey | None) -> str:
    delay_str = format_possible_delay(possible_delay)
    if alt_journey is None:
        return delay_template.format(delay = delay_str)

    alt_start_station = alt_journey[0].start
    alt_end_station = alt_journey[-1].end
    return (
        delay_template.format(delay = delay_str) +
        alt_journey_template.format(
            alt_departure_time = alt_start_station.public_departure,
            alt_arrival_time = alt_end_station.public_arrival))

