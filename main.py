import datetime
from knowledge_base.feeds import open_database
from reasoning_engine.routeing import find_journeys_from_crs
from reasoning_engine.tickets import ticket_prices

def main():
    db = open_database()
    from_location = 'BTN'
    to_location = 'CHM'
    date = datetime.date(2022, 1, 4)

    tickets = ticket_prices(db, from_location, to_location)
    if len(tickets) != 0:
        tickets.sort(key=lambda x: x[0])
        (cheapest_price, cheapest_ticket) = tickets[0]
        print(cheapest_price, cheapest_ticket.description)

    journeys = find_journeys_from_crs(db, from_location, to_location, date)
    for x in journeys:
        print(x[0].start.scheduled_departure_time, x[-1].end.scheduled_arrival_time)

if __name__ == '__main__':
    main()

