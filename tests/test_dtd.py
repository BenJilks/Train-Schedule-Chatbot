import unittest
from knowledge_base.open_rail import ticket_prices
from knowledge_base.dtd import open_dtd_database

class TestOpenRail(unittest.TestCase):
    
    def test_ticket_price(self):
        db = open_dtd_database()
        prices = ticket_prices(db, 'BTN', 'VIC')
        self.assertGreater(len(prices), 0)

