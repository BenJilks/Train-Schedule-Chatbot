import datetime
from knowledge_base.feeds import open_database
from reasoning_engine.delays import find_delays
from reasoning_engine.routeing import filter_best_journeys, find_journeys_from_crs
from reasoning_engine.tickets import ticket_prices

def main():
    db = open_database()

    # from_location = 'BTN'
    # to_location = 'CHM'
    from_location = 'MOG'
    to_location = 'SVG'
    date = datetime.date(2022, 1, 4)

    tickets = ticket_prices(db, from_location, to_location)
    if len(tickets) != 0:
        tickets.sort(key=lambda x: x[0])
        (cheapest_price, cheapest_ticket) = tickets[0]
        print(cheapest_price, cheapest_ticket.description)
    
    journeys = find_journeys_from_crs(db, from_location, to_location, date)
    best_journeys = filter_best_journeys(journeys, 10)
    possible_incidents = find_delays(db, best_journeys)
    for journey, incident in possible_incidents:
        print(journey[0].start.public_departure, incident.summery)

if __name__ == '__main__':
    main()

