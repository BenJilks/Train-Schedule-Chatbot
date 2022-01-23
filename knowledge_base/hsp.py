from __future__ import annotations
import datetime
import requests
import json
import sys
import config
from typing import Any, Union
from dataclasses import dataclass, fields
from enum import Enum, auto

def init_from_strings(instance: Any, **args):
    for attr in fields(type(instance)):
        if not attr.name in args:
            continue

        str_value = args[attr.name]
        real_value: Any = None
        if attr.type == 'int':
            real_value = int(str_value)
        elif attr.type == 'bool':
            real_value = (str_value == 'true')
        elif attr.type == 'str':
            real_value = str_value
        elif attr.type == 'datetime.time':
            real_value = datetime.datetime.strptime(str_value, '%H%M').time()
        elif attr.type == 'list[str]':
            real_value = str_value

        if real_value is None:
            raise Exception(f"Unsupported from string type '{ attr.type }'")
        setattr(instance, attr.name, real_value)

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
    from_date: datetime.date
    to_date: datetime.date
    days: HSPDays
    from_time: datetime.time = datetime.time(0, 0)
    to_time: datetime.time = datetime.time(23, 59)
    toc_filter: Union[list[str], None] = None

@dataclass(init=False)
class HSPDetails:
    location: str
    gbtt_ptd: datetime.time
    gbtt_pta: datetime.time
    actual_td: datetime.time
    actual_ta: datetime.time
    late_canc_reason: str

    def __init__(self, **args):
        init_from_strings(self, **args)

@dataclass(init=False)
class HSPAttributes:
    origin_location: str
    destination_location: str
    gbtt_ptd: datetime.time
    gbtt_pta: datetime.time
    toc_code: str
    matched_services: int
    rids: list[str]

    def __init__(self, **args):
        init_from_strings(self, **args)

@dataclass(init=False)
class HSPMetric:
    tolerance_value: int
    num_not_tolerance: int
    num_tolerance: int
    percent_tolerance: int
    global_tolerance: bool

    def __init__(self, **args):
        init_from_strings(self, **args)

@dataclass(init=False)
class HSPService:
    attributes: HSPAttributes
    metrics: list[HSPMetric]

    def __init__(self, serviceAttributesMetrics, Metrics):
        self.attributes = HSPAttributes(**serviceAttributesMetrics)
        self.metrics = [HSPMetric(**metric) for metric in Metrics]

    def time_late(self) -> Union[int, None]:
        if len(self.metrics) == 0:
            return None

        metric = max(self.metrics, key=lambda x: x.tolerance_value)
        if metric.num_not_tolerance == 0:
            return None
        
        return metric.tolerance_value

def hsp_route_statistics(from_crs: str, to_crs: str,
                         request: HSPRequest) -> list[HSPService]:
    data: dict[str, Any] = {
        'from_loc': from_crs,
        'to_loc': to_crs,
        'from_time': request.from_time.strftime('%H%M'),
        'to_time': request.to_time.strftime('%H%M'),
        'from_date': request.from_date.strftime('%Y-%m-%d'),
        'to_date': request.to_date.strftime('%Y-%m-%d'),
        'days': request.days.format(),
        'tolerance': ['0', '5', '10', '30'],
    }
    if not request.toc_filter is None:
        data['toc_filter'] = request.toc_filter

    headers = { "Content-Type": "application/json" }
    response = requests.post(
        config.HSP_SERVICE_METRICS_API_URL,
        auth=config.CREDENTIALS,
        headers=headers, json=data)

    try:
        result = []
        data = json.loads(response.text)
        for service in data['Services']:
            result.append(HSPService(**service))
        return result
    except Exception as exception:
        print(response.text, file=sys.stderr)
        raise exception

