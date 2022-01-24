import datetime
import math
import numpy
from dataclasses import dataclass
from typing import Iterable, Union
from knowledge_base.hsp import HSPDays, HSPRequest
from knowledge_base.hsp import hsp_route_statistics
from tensorflow.keras.models import Model, load_model
from tensorflow.nn import softmax

DelaysModel = Model

def date_to_float(date: datetime.date) -> float:
    date_int = date.year << 16 | date.month << 8 | date.day
    return float(date_int)

def time_to_float(time: datetime.time) -> float:
    time_int = time.hour << 8 | time.minute
    return float(time_int)

def crs_to_float(crs: str) -> float:
    return float(ord(crs[0]) << 16 | ord(crs[1]) << 8 | ord(crs[1]))

@dataclass
class TrainRouteStats:
    toc: str
    from_crs: str
    to_crs: str
    date: datetime.date
    departure_time: datetime.time
    arrival_time: datetime.time
    late_0m: int
    late_5m: int
    late_10m: int
    late_30m: int
    was_late_0m: bool
    was_late_5m: bool
    was_late_10m: bool
    was_late_30m: bool

    def as_model_input(self):
        return numpy.array([[
            float(self.late_0m),
            float(self.late_5m),
            float(self.late_10m),
            float(self.late_30m)]])

def sample_route_stats(from_crs: str, to_crs: str,
                       date: datetime.date, hour: int
                       ) -> Union[list[TrainRouteStats], None]:
    from_time = datetime.time(hour)
    to_time = datetime.time(hour + 1) if hour != 23 else datetime.time(hour, 59)
    today_stats = hsp_route_statistics(from_crs, to_crs, HSPRequest(
        from_date = date,
        to_date = date,
        from_time = from_time,
        to_time = to_time,
        days = HSPDays.from_date(date)))
    if today_stats is None:
        return None

    tocs = [service.attributes.toc_code for service in today_stats]
    last_week_stats = hsp_route_statistics(from_crs, to_crs, HSPRequest(
        from_date = date - datetime.timedelta(days=1, weeks=2),
        to_date = date - datetime.timedelta(days=1),
        from_time = from_time,
        to_time = to_time,
        days = HSPDays.from_date(date),
        toc_filter = tocs))
    if last_week_stats is None:
        return None
    
    training_data = []
    for service in last_week_stats:
        toc = service.attributes.toc_code
        depature_time = service.attributes.gbtt_ptd
        arrival_time = service.attributes.gbtt_pta
        late = {
            metric.tolerance_value: metric.num_not_tolerance
            for metric in service.metrics }

        today_trains = [train
            for train in today_stats
            if (train.attributes.toc_code == toc and
                train.attributes.gbtt_ptd == depature_time and
                train.attributes.gbtt_pta == arrival_time)]
        if len(today_trains) == 0:
            continue
        today_train = today_trains[0]
        time_late = today_train.time_late()
        training_data.append(TrainRouteStats(
            toc, from_crs, to_crs,
            date, depature_time, arrival_time,
            late[0], late[5], late[10], late[30],
            time_late == 0, time_late == 5,
            time_late == 10, time_late == 30))
    return training_data

def open_delays_model(file_path: str) -> DelaysModel:
    return load_model(file_path)

def get_stat_at(stats: Iterable[TrainRouteStats],
                departure_time: datetime.time) -> Union[TrainRouteStats, None]:
    closest = None
    closest_offset = math.inf
    for stat in stats:
        hour_offset = abs(stat.departure_time.hour - departure_time.hour)
        minute_offset = abs(stat.departure_time.minute - departure_time.minute)
        offset = hour_offset * 60 + minute_offset
        if offset < closest_offset:
            closest = stat
            closest_offset = offset
    return closest

def delay_for_route(model: DelaysModel,
                    from_crs: str, to_crs: str,
                    date: datetime.date,
                    depature_time: datetime.time) -> Union[int, None]:
    stats = sample_route_stats(from_crs, to_crs, date, depature_time.hour)
    if stats is None:
        return None

    stat = get_stat_at(stats, depature_time)
    if stat is None:
        return None

    try:
        prediction = model(stat.as_model_input()).numpy()
        probability = softmax(prediction).numpy()[0]
        max_prob = max(probability)
        if max_prob < 0.5:
            return None

        max_index = probability.index(max_prob)
        if max_index == 0: return 0
        if max_index == 1: return 5
        if max_index == 2: return 10
        if max_index == 3: return 30
    except:
        pass

    return None

