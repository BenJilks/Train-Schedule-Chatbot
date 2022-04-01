import datetime
import regex
import config
from dataclasses import dataclass
from enum import Enum, auto
from typing import Union
from sqlalchemy.orm.session import Session
from interface.bot import ConversationState
from knowledge_base.dtd import TIPLOC, TicketType
from knowledge_base.kb import Incident, Station
from reasoning_engine.routeing import Journey

pending_response_template = (
"""
Gathering route information from {from_loc} to {to_loc}. On {date} from {time}
""")

journey_template = (
"""
The latest train will be leaving {from_loc} at {departure_time}, it will arrive at {to_loc} at {arrival_time}.
With {stops} stops.
{ticket_type} single ticket: {single_ticket_price}
{ticket_type} return ticket: {return_ticket_price}
{link}
""")

alt_journey_template = (
"""
An alternative journey departs at {alt_departure_time} and arrives at {alt_arrival_time}.
"""
)

delay_template = (
"""
The train is expected to be {delay}.
""")

incidents_template = (
"""
Incidents that may affect your journey:
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
    alt_journey: Union[Journey, None]
    incidents: list[Incident]
    tickets: list[tuple[int, TicketType]]

class TicketFor(Enum):
    Adult = auto()
    Child = auto()

@dataclass
class RoutePlanningState(ConversationState):
    from_loc: Union[UserLocation, None] = None
    to_loc: Union[UserLocation, None] = None
    date: datetime.date = datetime.date.today()
    time: datetime.time = datetime.datetime.now().time()
    request_incidents: bool = False
    request_weather: bool = False
    request_stops: bool = False
    rerequest_tickets: bool = False
    request_delays: bool = False
    ticket_for: TicketFor = TicketFor.Adult
    user_info: Union[UserInfo, None] = None

def format_pending_response(state: RoutePlanningState) -> str:
    assert not state.from_loc is None
    assert not state.to_loc is None
    return pending_response_template.format(
        from_loc = state.from_loc.name,
        to_loc = state.to_loc.name,
        date = state.date.strftime(config.STANDARD_DATE_FORMAT),
        time = state.time.strftime(config.STANDARD_TIME_FORMAT))

def format_price(price: Union[int, None]) -> str:
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

def format_not_enough_data_response(state: RoutePlanningState) -> str:
    if not state.from_loc is None:
        return f'\nPlease tell me where you\'re planning to go from { state.from_loc.name }'
    return '\nPlease tell me where you want to traval from and to'

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
        departure_time = start_station.public_departure.strftime(config.STANDARD_TIME_FORMAT),
        arrival_time = end_station.public_arrival.strftime(config.STANDARD_TIME_FORMAT),
        stops = len(journey) - 1,
        ticket_type = 'Adult' if state.ticket_for == TicketFor.Adult else 'Child',
        single_ticket_price = format_price(cheapest_single),
        return_ticket_price = format_price(cheapest_return),
        link = get_link(state))

def generate_names_to_crs_map(db: Session) -> dict[str, str]:
    if 'cache' in generate_names_to_crs_map.__dict__:
        return generate_names_to_crs_map.cache

    generate_names_to_crs_map.cache = { name: crs
        for name, crs in db.query(Station.name, Station.crs_code).all() }
    return generate_names_to_crs_map.cache

def generate_tiploc_to_names_map(db: Session) -> dict[str, str]:
    if 'cache' in generate_tiploc_to_names_map.__dict__:
        return generate_tiploc_to_names_map.cache

    generate_tiploc_to_names_map.cache = { tiploc: name
        for name, tiploc in db.query(Station.name, TIPLOC.tiploc_code)\
            .select_from(Station, TIPLOC)\
            .filter(TIPLOC.crs_code == Station.crs_code)
            .all() }
    return generate_tiploc_to_names_map.cache

def format_stops_response(db: Session, state: RoutePlanningState, journey: Journey) -> str:
    assert not state.from_loc is None
    assert not state.to_loc is None
    tiploc_to_names_map = generate_tiploc_to_names_map(db)

    response = f'\nStops in journey from { state.from_loc.name } to { state.to_loc.name }:'
    for segment in journey:
        from_name = tiploc_to_names_map[segment.start.location]
        from_time = segment.start.public_departure.strftime(config.STANDARD_TIME_FORMAT)
        to_name = tiploc_to_names_map[segment.end.location]
        to_time = segment.end.public_arrival.strftime(config.STANDARD_TIME_FORMAT)
        response += f'\n{ from_name } { from_time } -> { to_name } { to_time }'
    return response

def format_incidents_response(incidents: list[Incident]) -> str:
    incidents_str = '\n'.join([
        incident.summery for incident in incidents[:2]])
    response = incidents_template.format(
        incidents = incidents_str)

    if len(incidents) > 2:
        response += f'And { len(incidents) - 2 } more\n'

    response += 'https://www.nationalrail.co.uk/indicator.aspx'
    return response

def format_possible_delay(possible_delay: Union[int, None]) -> str:
    if possible_delay is None:
        return 'on time'
    return f'up to { possible_delay } minutes late'

def format_delays_response(possible_delay: Union[int, None],
                           alt_journey: Union[Journey, None]) -> str:
    delay_str = format_possible_delay(possible_delay)
    if alt_journey is None:
        return delay_template.format(delay = delay_str)

    alt_start_station = alt_journey[0].start
    alt_end_station = alt_journey[-1].end
    return (
        delay_template.format(delay = delay_str) +
        alt_journey_template.format(
            alt_departure_time = alt_start_station.public_departure.strftime(config.STANDARD_TIME_FORMAT),
            alt_arrival_time = alt_end_station.public_arrival.strftime(config.STANDARD_TIME_FORMAT)))

def gather_locations(db: Session, message: str, state: RoutePlanningState):
    name_crs_map = generate_names_to_crs_map(db)
    lower_message = f' { message.lower() } '
    locations_in_message = [
        (name, crs, lower_message.index(name.lower()))
        for name, crs in name_crs_map.items()
        if f' { name.lower() } ' in lower_message]

    locations_in_message.sort(key = lambda x: x[2])
    for name, crs, _ in locations_in_message:
        if state.from_loc is None:
            state.from_loc = UserLocation(crs, name)
        elif state.to_loc is None:
            state.to_loc = UserLocation(crs, name)

def next_date_on_day(day: int) -> datetime.date:
    curr = datetime.date.today()
    while True:
        curr += datetime.timedelta(days=1)
        if curr.weekday() == day:
            return curr

def find_possible_time_str(options: list[tuple[str, str]],
                           text: str) -> Union[datetime.datetime, None]:
    for regex_pattern, format_pattern in options:
        pos = 0
        while True:
            match = regex.search(regex_pattern, text, pos = pos)
            if match is None:
                break

            try:
                date_str = match.group()\
                    .replace('st', '').replace('nd', '')\
                    .replace('rd', '').replace('th', '')\
                    .replace('am', 'AM').replace('pm', 'PM')

                date = datetime.datetime.strptime(date_str, format_pattern)
                if date.year == 1900:
                    today = datetime.datetime.now()
                    date = date.replace(year = today.year)
                    if date < today:
                        date = date.replace(year = today.year + 1)
                return date
            except:
                pass

            pos = match.start() + 1
    return None

def gather_dates(message: str, state: RoutePlanningState):
    lower_message = message.lower()
    if 'tomorrow' in lower_message:
        state.date = datetime.date.today() + datetime.timedelta(days=1)
        return

    for day, day_name in enumerate(config.DAYS_OF_WEEK):
        if day_name.lower() in lower_message:
            state.date = next_date_on_day(day)
    
    possible_date_strs = ([
        ('[0-9]{2}-[0-9]{2}-[0-9]{4}', '%d-%m-%Y'),
        ('[0-9]{2}-[0-9]{2}-[0-9]{2}', '%d-%m-%y'),
        ('[0-9]{2}/[0-9]{2}/[0-9]{4}', '%d/%m/%Y'),
        ('[0-9]{2}/[0-9]{2}/[0-9]{2}', '%d/%m/%y')] +
        sum(
            [[
                (f'[0-9]+{ suffix } of [A-Za-z]+{ regex_year }', f'%d of %B{ format_year }'),
                (f'[A-Za-z]+ the [0-9]+{ suffix }{ regex_year }', f'%B the %-d{ format_year }'),
                (f'[A-Za-z]+ [0-9]+{ suffix }{ regex_year }', f'%B %d{ format_year }')]
            for regex_year, format_year in [(' [0-9]{4}', ' %Y'), (' [0-9]{2}', ' %y'), ('', '')]
            for suffix in ['st', 'nd', 'rd', 'th']],
            start = []))

    date = find_possible_time_str(possible_date_strs, message)
    if not date is None:
        state.date = date.date()

def decode_time_str(time_str: str) -> Union[datetime.time, None]:
    possible_time_formats = ['%I:%M %p', '%I %p', '%-I %p', '%H:%M', '%H', '%-H']
    for time_format in possible_time_formats:
        try:
            time = datetime.datetime.strptime(time_str, time_format).time()
            return time
        except:
            continue
    return None

def gather_times(message: str, state: RoutePlanningState):
    possible_time_strs = [
        ('[0-9]+ am', '%I %p'),
        ('[0-9]+ pm', '%I %p'),
        ('[0-9]{2}:[0-9]{2}', '%H:%M'),
        ('[0-9]{2}:[0-9]{2} am', '%I:%M %p'),
        ('[0-9]{2}:[0-9]{2} pm', '%I:%M %p')]

    time = find_possible_time_str(possible_time_strs, message)
    if not time is None:
        state.time = time.time()

def gather_extra_request(message: str, state: RoutePlanningState):
    lower_message = message.lower()
    if 'incident' in lower_message:
        state.request_incidents = True
    if 'weather' in lower_message or 'forecast' in lower_message:
        state.request_weather = True
    if 'stop' in lower_message or 'change over' in lower_message:
        state.request_stops = True
    if 'delay' in lower_message or 'alternative' in lower_message:
        state.request_delays = True
    if 'adult' in lower_message:
        state.ticket_for = TicketFor.Adult
        state.rerequest_tickets = True
    if 'child' in lower_message:
        state.ticket_for = TicketFor.Child
        state.rerequest_tickets = True

def gather_information(db: Session, message: str, state: RoutePlanningState):
    gather_locations(db, message, state)
    gather_dates(message, state)
    gather_times(message, state)
    gather_extra_request(message, state)

