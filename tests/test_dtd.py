import datetime
import unittest
from knowledge_base.open_rail import ticket_prices, station_timetable
from knowledge_base.dtd import open_dtd_database

class TestOpenRail(unittest.TestCase):
    
    def test_ticket_price(self):
        db = open_dtd_database()
        prices = ticket_prices(db, 'BTN', 'VIC')
        self.assertGreater(len(prices), 0)

    def test_station_timetable(self):
        db = open_dtd_database()
        date = datetime.date(2022, 1, 4)
        timetable = station_timetable(db, 'BTN', date)
        self.assertGreater(len(timetable), 0)

        times = [x.scheduled_arrival_time 
                for x in timetable 
                if not x.scheduled_arrival_time is None]
        self.assertGreater(len(times), 0)

