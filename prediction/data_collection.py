import os, sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))

import datetime
import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from calendar import monthrange
from random import randint
from typing import Iterator
from reasoning_engine.delays import TrainRouteStats, sample_route_stats
from knowledge_base.dtd import TIPLOC, TimetableLink
from knowledge_base.feeds import open_database
from sqlalchemy.orm.session import Session
from sqlalchemy.orm.util import aliased
from sqlalchemy.sql import func

def possible_routes(db: Session, count: int) -> list[tuple[str, str]]:
    from_location = aliased(TIPLOC)
    to_location = aliased(TIPLOC)
    return list(db.query(from_location.crs_code, to_location.crs_code)\
        .select_from(TimetableLink)
        .join(from_location, from_location.tiploc_code == TimetableLink.from_location)\
        .join(to_location, to_location.tiploc_code == TimetableLink.to_location)\
        .filter(from_location.crs_code != '')\
        .filter(to_location.crs_code != '')\
        .order_by(func.random())\
        .limit(count)\
        .all())

def generate_training_sample(from_crs: str, to_crs: str) -> Iterator[TrainRouteStats]:
    year = randint(2018, 2022)
    month = randint(1, 12)
    day = randint(1, monthrange(year, month)[1])
    date = datetime.date(year, month, day)
    hour = randint(0, 22)
    try:
        yield from sample_route_stats(from_crs, to_crs, date, hour)
    except Exception as e:
        print(e)
        time.sleep(60 * 10)

def generate_training_data_samples(routes: list[tuple[str, str]],
                                   ) -> list[TrainRouteStats]:
    result = []
    for from_crs, to_crs in routes:
        result += generate_training_sample(from_crs, to_crs)
    return result

def start_collector_threads(db: Session, count: int,
                            thread_count: int) -> Iterator[TrainRouteStats]:
    with ThreadPoolExecutor() as executor:
        tasks = []
        for _ in range(thread_count):
            routes = list(possible_routes(db, count))
            tasks.append(executor.submit(generate_training_data_samples, routes))
        
        for task in as_completed(tasks):
            yield from task.result()

def csv_line_for_training_entry(entry: TrainRouteStats) -> str:
    return ','.join([
        entry.toc, entry.from_crs, entry.to_crs,
        entry.date.isoformat(), entry.departure_time.isoformat(), entry.arrival_time.isoformat(),
        str(entry.late_0m), str(entry.late_5m), str(entry.late_10m), str(entry.late_30m),
        str(entry.was_late_0m), str(entry.was_late_5m), str(entry.was_late_10m), str(entry.was_late_30m)])

def main():
    parser = argparse.ArgumentParser(description='Collect training data')
    parser.add_argument('--count', '-c', help='Number of samples to collect per thread', required=True)
    parser.add_argument('--thread-count', '-t', help='Number of threads to use', default=1)
    parser.add_argument('--output', '-o', help='Path of output csv file', required=True)
    args = parser.parse_args()

    db = open_database()
    with open(args.output, 'a') as csv_file:
        for entry in start_collector_threads(db, int(args.count), int(args.thread_count)):
            csv_line = csv_line_for_training_entry(entry)
            csv_file.write(f'{ csv_line }\n')
            csv_file.flush()

if __name__ == '__main__':
    main()

