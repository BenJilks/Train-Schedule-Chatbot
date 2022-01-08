import datetime
import unittest
from knowledge_base.open_rail import ticket_prices, find_routes
from knowledge_base.dtd import open_dtd_database

class TestOpenRail(unittest.TestCase):
    
    def test_ticket_price(self):
        db = open_dtd_database()
        prices = ticket_prices(db, 'BTN', 'PRP')
        self.assertGreater(len(prices), 0)

    def test_find_routes(self):
        db = open_dtd_database()
        date = datetime.date(2022, 1, 4)
        routes = find_routes(db, 'BTN', 'PRP', date)

        self.assertGreater(len(routes), 0)
        largest_time = None
        for route in routes:
            time = route.end.scheduled_arrival_time
            if not largest_time is None:
                self.assertGreater(time, largest_time)
            largest_time = max(time, largest_time or time)

