import requests
import json
from datetime import date
from dataclasses import dataclass

API_URL = 'https://hsp-prod.rockshore.net/api/v1/serviceMetrics'
HEADERS = { 'Content-Type': 'application/json' }
CREDENTIALS = ('--YOUR EMAIL ADDRESS--', '--YOUR PASSWORD--')

@dataclass
class Request:
    from_location: str
    to_location: str
    time_minutes_range: tuple[int, int]
    date_range: tuple[date, date]

def minutes_to_hhmm_format(minutes: int) -> str:
    return f"{ int(minutes / 60) }{ minutes % 60 }"

def make_request(request: Request) -> object:
    (from_time_minutes, to_time_minutes) = request.time_minutes_range
    (from_date, to_date) = request.date_range

    request_data = {
      'from_loc': request.from_location,
      'to_loc': request.to_location,
      'from_time': minutes_to_hhmm_format(from_time_minutes),
      'to_time': minutes_to_hhmm_format(to_time_minutes),
      'from_date': from_date.isoformat(),
      'to_date': to_date.isoformat(),
    }

    response = requests.post(API_URL, headers=HEADERS, auth=CREDENTIALS, json=request_data)
    response_json = json.loads(response.text)

    # TODO: Have this be parsed into a dataclass
    return response_json['Services']

def available_trains(from_location: str, to_location: str, 
                     travel_date: date, time_minutes_range: tuple[int, int]) -> object:
    request = Request(
        from_location=from_location, 
        to_location=to_location, 
        time_minutes_range=time_minutes_range,
        date_range=(travel_date, travel_date))
    return make_request(request)

