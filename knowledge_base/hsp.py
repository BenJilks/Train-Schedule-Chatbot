from __future__ import annotations
import datetime
import requests
import json
from typing import Iterator
from dataclasses import dataclass
from enum import Enum, auto
from sqlalchemy.orm.session import Session
from knowledge_base import TrainRoute, config
from knowledge_base import tiploc_route_to_crs_route

class HSPDays(Enum):
    Weekday = auto()
    Saturday = auto()
    Sunday = auto()

    @staticmethod
    def from_date(date: datetime.date) -> HSPDays:
        weekday = date.weekday()
        if weekday == 5:
            return HSPDays.Saturday
        if weekday == 6:
            return HSPDays.Sunday
        return HSPDays.Weekday

    def format(self) -> str:
        table = {
            HSPDays.Weekday: 'WEEKDAY',
            HSPDays.Saturday: 'SATURDAY',
            HSPDays.Sunday: 'SUNDAY',
        }
        return table[self]

@dataclass
class HSPRequest:
    from_time: datetime.time
    to_time: datetime.time
    from_date: datetime.date
    to_date: datetime.date
    days: HSPDays

@dataclass
class HSPDetails:
    location: str
    gbtt_ptd: datetime.time
    gbtt_pta: datetime.time
    actual_td: datetime.time
    actual_ta: datetime.time
    late_canc_reason: str

def train_details_for_rid(rid: str) -> Iterator[HSPDetails]:
    data = { 'rid': rid }
    headers = { "Content-Type": "application/json" }
    response = requests.post(
        config.HSP_SERVICE_DETAILS_API_URL,
        auth=config.CREDENTIALS,
        headers=headers,
        json=data)
    
    def parse_time(time_str: str) -> datetime.time:
        return datetime.datetime.strptime(time_str, '%H%I').time()

    data = json.loads(response.text)
    details = data['serviceAttributesDetails']
    for location in details['locations']:
        yield HSPDetails(
            location['location'],
            parse_time(location['gbtt_ptd']),
            parse_time(location['gbtt_ptd']),
            parse_time(location['gbtt_ptd']),
            parse_time(location['gbtt_ptd']),
            location['late_canc_reason'])

def train_details_for_segment(from_crs: str, to_crs: str,
                              request: HSPRequest) -> Iterator[HSPDetails | str]:
    data = {
        'from_loc': from_crs,
        'to_loc': to_crs,
        'from_time': request.from_time.strftime('%H%I'),
        'to_time': request.to_time.strftime('%H%I'),
        'from_date': request.from_date.strftime('%Y-%m-%d'),
        'to_date': request.to_date.strftime('%Y-%m-%d'),
        'days': request.days.format(),
    }

    headers = { "Content-Type": "application/json" }
    response = requests.post(
        config.HSP_SERVICE_METRICS_API_URL,
        auth=config.CREDENTIALS,
        headers=headers,
        json=data)

    try:
        data = json.loads(response.text)
        for service in data['Services']:
            metrics = service['serviceAttributesMetrics']
            rids = metrics['rids']
            for rid in rids:
                yield from train_details_for_rid(rid)
    except:
        yield response.text

def hsp_data_for_train_route(db: Session, train_route: TrainRoute, 
                             request: HSPRequest) -> Iterator[Iterator[HSPDetails | str]]:
    for segment in train_route:
        start, stop = tiploc_route_to_crs_route(db,
            [segment.start_location, segment.stop_location])
        yield train_details_for_segment(start, stop, request)

