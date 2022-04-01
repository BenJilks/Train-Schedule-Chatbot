import datetime
import config
from pyowm.owm import OWM
from typing import Any, Callable, Union
from interface.response import RoutePlanningState, UserInfo, gather_information
from interface.response import format_stops_response
from interface.response import format_delays_response
from interface.response import format_not_enough_data_response
from interface.response import format_pending_response
from interface.response import format_journey_response
from interface.response import format_incidents_response
from knowledge_base import TrainRoute, tiploc_route_to_crs_route
from knowledge_base.feeds import open_database
from knowledge_base.weather import get_weather_at_crs, open_weather
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

def fetch_and_report_route_info(on_response: Callable[[str], None],
                                db: Session, model: Model,
                                state: RoutePlanningState) -> Union[UserInfo, None]:
    assert not state.from_loc is None
    assert not state.to_loc is None
    on_response(format_pending_response(state))

    # Route journeys
    print('Finding route')
    journeys = find_journeys(db, 
        state.from_loc.crs, state.to_loc.crs,
        state.date, state.time)

    if len(journeys) == 0:
        # Try looking at the start of tomorrow
        journeys = find_journeys(db, state.from_loc.crs, state.to_loc.crs,
            state.date + datetime.timedelta(days=1), datetime.time(0))

        if len(journeys) == 0:
            on_response(f'No route from { state.from_loc.name } to { state.to_loc.name } found')
            return None

    # Ticket and journey info
    print('Reporting on journey info')
    _, journey = journeys[0]
    tickets = ticket_prices(db, state.from_loc.crs, state.to_loc.crs)
    on_response(format_journey_response(state, journey, tickets))

    # Incidents
    print('Gathering incidents')
    possible_incidents = find_incidents(db, journeys)
    if len(possible_incidents) != 0:
        on_response(format_incidents_response(possible_incidents))

    # Alt route
    _, alt_journey = journeys[1] if len(journeys) > 1 else None, None

    print('Done')
    return UserInfo(
        state.from_loc, state.to_loc, journey, 
        alt_journey, possible_incidents, tickets)

def handle_conversation_state(text: str,
                              on_response: Callable[[str], None],
                              state: RoutePlanningState,
                              db: Session,
                              model: Model,
                              owm: OWM):
    raw_text_message_content = strip_html(text)
    gather_information(db, raw_text_message_content, state)
    print(f'Got message { raw_text_message_content }')

    lower_message = text.lower()
    if 'hi ' in lower_message or 'hi!' in lower_message or 'hello' in lower_message:
        on_response('Hi!')

    if not has_enough_info_for_user_report(state):
        return on_response(format_not_enough_data_response(state))

    if state.user_info is None or state.rerequest_tickets:
        state.user_info = fetch_and_report_route_info(on_response, db, model, state)
        state.rerequest_tickets = False
    
    if not state.user_info is None and state.request_delays:
        print('Predicting delays request')
        possible_delay = find_delays(db, model, state.user_info.journey, state.date)
        on_response(format_delays_response(possible_delay, state.user_info.alt_journey))
        state.request_delays = False

    if not state.user_info is None and state.request_incidents:
        print('Incidents requested')
        if len(state.user_info.incidents) == 0:
            on_response('No incidents to report')
        for incident in state.user_info.incidents:
            on_response(strip_html(incident.description))
        state.request_incidents = False

    if not state.from_loc is None and state.request_weather:
        print('Weather requested')
        date_and_time = datetime.datetime.combine(state.date, state.time)
        date_time_str = date_and_time.strftime(config.STANDARD_DATE_TIME_FORMAT)
        weather = get_weather_at_crs(db, owm, date_and_time, state.from_loc.crs)
        if weather is None:
            on_response(
                f'\nNo forecast for { state.from_loc.name } on { date_time_str } available')
        else:
            on_response(
                f'\nThe weather at { state.from_loc.name } on { date_time_str } will be { weather }')
        state.request_weather = False

    if not state.user_info is None and state.request_stops:
        print('Stops requested')
        on_response(format_stops_response(
            db, state, state.user_info.journey))
        state.request_stops = False

def handle_bot_conversation_state(bot: Mastodon,
                                  message: Message, 
                                  state: RoutePlanningState,
                                  db: Session,
                                  model: Model,
                                  owm: OWM) -> Union[Message, None]:
    last_message = message
    def on_response(response: str):
        nonlocal last_message
        last_message = send_reply(bot, last_message, response)

    handle_conversation_state(message.text,
        on_response, state, db, model, owm)
    return last_message

def text_mode(db: Session, model: Model, owm: OWM):
    print(' !!! WARNING: Mastodon disabled, running in text mode !!! ')
    state = RoutePlanningState()
    while True:
        text = input('>> ')
        if text.strip().lower() in ['exit', 'quit', 'q']:
            break

        handle_conversation_state(text,
            print, state, db, model, owm)

def main():
    print(' ==> Loading data')
    db = open_database()
    model = open_delays_model('prediction/delays.model')
    owm = open_weather()

    if config.DISABLE_MASTODON:
        text_mode(db, model, owm)
        return

    print(' ==> Listening for messages')
    bot = open_bot()
    conversation_handler(bot, RoutePlanningState,
        handle_bot_conversation_state,
        application_state = [db, model, owm])

if __name__ == '__main__':
    main()

