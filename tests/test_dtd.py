import datetime
import unittest
from knowledge_base.open_rail import ticket_prices, station_timetable
from knowledge_base.dtd import open_dtd_database

class TestOpenRail(unittest.TestCase):
    
    def test_ticket_price(self):
        db = open_dtd_database()
        prices = ticket_prices(db, 'BTN', 'PRP')
        self.assertGreater(len(prices), 0)

    def test_station_timetable(self):
        db = open_dtd_database()
        date = datetime.date(2022, 1, 4)
        timetable = station_timetable(db, 'BTN', 'PRP', date)

        self.assertGreater(len(timetable), 0)
        self.assertTrue(all([
            not next_station is None 
            for _, next_station in timetable]))

