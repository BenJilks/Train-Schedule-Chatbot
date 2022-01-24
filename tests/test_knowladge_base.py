import tempfile
import unittest
from typing import Any
from knowledge_base.feeds import ExpiryTimes, open_database
from knowledge_base.kb import Incident, KBIncidents
from sqlalchemy.sql import func
from sqlalchemy.orm.session import Session

class KnowladgeBase(unittest.TestCase):
    
    def assert_incidents_valid(self, db: Session):
        count = db.query(func.count(Incident.incident_number)).first()
        self.assertIsNotNone(count)
        if not count is None:
            self.assertGreater(count[0], 0)

    def remove_all_incidents(self, db: Session):
        db.query(Incident).delete()

    def reset_incident_expiry(self, db: Session):
        incident_api_url = KBIncidents().feed_api_url()
        
        entry = db.query(ExpiryTimes).get(incident_api_url)
        self.assertIsNotNone(entry)
        if entry is None:
            return

        expiry_time: Any = entry
        expiry_time.expiry_timestamp = 0
        db.commit()

    def test_feed_expire(self):
        temp_file_path = tempfile.mktemp()
        with open(temp_file_path, 'w') as temp_file:
            db = open_database(file = temp_file)
            self.assert_incidents_valid(db)
            self.remove_all_incidents(db)
            self.reset_incident_expiry(db)
            db.close()

            db = open_database(file = temp_file)
            self.assert_incidents_valid(db)
            db.close()

