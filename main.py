from knowledge_base.dtd import open_dtd_database
from knowledge_base.open_rail import find_best_journeys_from_crs, ticket_prices
import datetime

def main():
    db = open_dtd_database()
    from_location = 'BTN'
    to_location = 'CHM'
    date = datetime.date(2022, 1, 4)

    tickets = ticket_prices(db, from_location, to_location)
    if len(tickets) != 0:
        tickets.sort(key=lambda x: x[0])
        (cheapest_price, cheapest_ticket) = tickets[0]
        print(cheapest_price, cheapest_ticket.description)

    best_journeys = find_best_journeys_from_crs(db, from_location, to_location, date)
    for x in best_journeys:
        print(x[0].start.scheduled_departure_time, x[-1].end.scheduled_arrival_time)

if __name__ == '__main__':
    main()

