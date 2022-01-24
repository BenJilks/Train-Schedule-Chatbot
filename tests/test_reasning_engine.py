import datetime
import unittest
from reasoning_engine.tickets import ticket_prices
from reasoning_engine.routeing import filter_best_journeys, find_journeys_from_crs
from knowledge_base.feeds import open_database

class ReasoningEngine(unittest.TestCase):
    
    def test_ticket_price(self):
        db = open_database()
        prices = ticket_prices(db, 'BTN', 'PRP')
        self.assertGreater(len(prices), 0)

    def test_find_routes(self):
        db = open_database()
        date = datetime.date(2022, 1, 4)
        route_journeys = find_journeys_from_crs(db, 'BTN', 'PRP', date)
        best_route_journeys = list(filter_best_journeys(route_journeys))
        self.assertGreater(len(best_route_journeys), 0)

