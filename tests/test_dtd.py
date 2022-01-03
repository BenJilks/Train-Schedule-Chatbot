import unittest
from knowledge_base.open_rail import ticket_prices
from knowledge_base.dtd import open_dtd_database

class TestOpenRail(unittest.TestCase):
    
    def test_ticket_price(self):
        db = open_dtd_database()
        prices = ticket_prices(db, 'BTN', 'VIC')
        self.assertGreater(len(prices), 0)

        for price, ticket in prices:
            print(price)
            print(ticket.description)
            print(ticket.tkt_type)
            print(ticket.tkt_class)
            print(ticket.tkt_group)
            print()

