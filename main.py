import datetime
from typing import Callable, Union
from interface.response import RoutePlanningState, UserInfo, UserLocation, format_delays_response
from interface.response import format_pending_response
from interface.response import format_journey_response
from interface.response import format_incidents_response
from knowledge_base import TrainRoute, tiploc_route_to_crs_route
from knowledge_base.feeds import open_database
from knowledge_base.kb import Station
from reasoning_engine.delays import delay_for_route, open_delays_model
from reasoning_engine.incidents import find_incidents, strip_html
from reasoning_engine.routeing import Journey, filter_best_journeys, find_journeys_from_crs
from reasoning_engine.tickets import ticket_prices
from interface.bot import Message, open_bot, send_reply
from interface.bot import conversation_handler
from sqlalchemy.orm.session import Session
from tensorflow.keras.models import Model
from mastodon.Mastodon import Mastodon

def has_enough_info_for_user_report(state: RoutePlanningState) -> bool:
    return (
        (not state.from_loc is None) and
        (not state.to_loc is None))

def order_key(journey: Journey) -> datetime.time:
    departure = journey[0].start.public_departure
    arrival = journey[-1].end.public_arrival
    if arrival < departure:
        return departure
    else:
        return arrival

def find_journeys(db: Session, from_crs: str, to_crs: str,
                  date: datetime.date, time: datetime.time) -> list[tuple[TrainRoute, Journey]]:
    journeys = find_journeys_from_crs(db, from_crs, to_crs, date)
    best_journeys = filter_best_journeys(journeys)

    route_journeys = [
        (route, journey)
        for route, journeys in best_journeys
        for journey in journeys
        if journey[0].start.public_departure >= time]
    route_journeys.sort(key = lambda x: order_key(x[1]))
    return route_journeys

def find_delays(db: Session, model: Model, journey: Journey, date: datetime.date) -> Union[int, None]:
    start_segment = journey[0]
    start_tiploc = start_segment.start.location
    end_tiploc = start_segment.end.location
    departure_time = start_segment.start.public_departure

    start_crs, stop_crs = tiploc_route_to_crs_route(db, start_tiploc, end_tiploc)
    return delay_for_route(model, start_crs, stop_crs, date, departure_time)

def fetch_and_report_route_info(on_report: Callable[[str], None],
                                db: Session, model: Model,
                                state: RoutePlanningState) -> UserInfo:
    assert not state.from_loc is None
    assert not state.to_loc is None
    on_report(format_pending_response(state))

    # Route journeys
    print('Finding route')
    journeys = find_journeys(db, 
        state.from_loc.crs, state.to_loc.crs,
        state.date, state.time)

    # Ticket and journey info
    print('Reporting on journey info')
    _, journey = journeys[0]
    print(journey[0].train.toc)
    tickets = ticket_prices(db, state.from_loc.crs, state.to_loc.crs)
    on_report(format_journey_response(state, journey, tickets))

    # Incidents
    print('Gathering incidents')
    possible_incidents = find_incidents(db, journeys)
    if len(possible_incidents) != 0:
        on_report(format_incidents_response(possible_incidents))

    # Delays
    print('Predicting delays')
    _, alt_journey = journeys[1] if len(journeys) > 1 else None, None
    possible_delay = find_delays(db, model, journey, state.date)
    on_report(format_delays_response(possible_delay, alt_journey))

    print('Done')
    return UserInfo(
        state.from_loc, state.to_loc,
        journey, alt_journey,
        possible_incidents, possible_delay,
        tickets)

def generate_names_to_crs_map(db: Session) -> dict[str, str]:
    if 'cache' in generate_names_to_crs_map.__dict__:
        return generate_names_to_crs_map.cache

    generate_names_to_crs_map.cache = { name: tiploc
        for name, tiploc in db.query(Station.name, Station.crs_code).all() }
    return generate_names_to_crs_map.cache

def gather_information(db: Session, message: str, state: RoutePlanningState):
    name_crs_map = generate_names_to_crs_map(db)
    lower_message = message.lower()
    locations_in_message = [
        (name, crs, lower_message.index(name.lower()))
        for name, crs in name_crs_map.items()
        if name.lower() in lower_message]

    locations_in_message.sort(key = lambda x: x[1])
    for name, crs, _ in locations_in_message:
        if state.from_loc is None:
            state.from_loc = UserLocation(crs, name)
        elif state.to_loc is None:
            state.to_loc = UserLocation(crs, name)

def handle_bot_conversation_state(bot: Mastodon,
                                  message: Message, 
                                  state: RoutePlanningState,
                                  db: Session,
                                  model: Model) -> Union[Message, None]:
    raw_text_message_content = strip_html(message.text)
    gather_information(db, raw_text_message_content, state)
    print(f'Got message { raw_text_message_content }')

    if not has_enough_info_for_user_report(state):
        return send_reply(bot, message, '<< Not enough info message >>')

    last_message = message
    if state.user_info is None:
        def reply(message: str):
            nonlocal last_message
            last_message = send_reply(bot, last_message, message)
        state.user_info = fetch_and_report_route_info(reply, db, model, state)
    
    if not state.user_info is None and state.request_incidents:
        last_message = message
        for incident in state.user_info.incidents:
            last_message = send_reply(bot, last_message, incident.description)

    return last_message

def main():
    print(' ==> Loading data')
    db = open_database()
    model = open_delays_model('prediction/delays.model')

    if False:
        fetch_and_report_route_info(print, db, model, RoutePlanningState(
            from_loc = UserLocation('CHM', 'Chelmsford'),
            to_loc = UserLocation('COL', 'Colchester')))
    else:
        print(' ==> Listening for messages')
        bot = open_bot()
        conversation_handler(bot, RoutePlanningState,
            handle_bot_conversation_state,
            application_state = [db, model])

if __name__ == '__main__':
    main()

