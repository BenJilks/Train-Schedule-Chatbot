from __future__ import annotations
from dataclasses import dataclass
from itertools import groupby
from typing import Any, Callable, Iterable, Protocol, TypeVar
from sqlalchemy.orm.session import Session
from knowledge_base.dtd import TIPLOC

class _SupportsLessThan(Protocol):
    def __lt__(self, __other: Any) -> bool: ...

_SupportsLessThanT = TypeVar("_SupportsLessThanT", bound=_SupportsLessThan)
_T = TypeVar('_T')
def group(it: Iterable[_T], key: Callable[[_T], _SupportsLessThanT]) -> dict[_SupportsLessThanT, list[_T]]:
    return { k: list(g) for k, g in groupby(sorted(it, key=key), key=key) }

def tiploc_route_to_crs_route(db: Session, *tiploc_route: Iterable[str]) -> list[str]:
    tiploc_to_crs_map = {
        tiploc: crs
        for tiploc, crs in db\
            .query(TIPLOC.tiploc_code, TIPLOC.crs_code)\
            .filter(TIPLOC.tiploc_code.in_(tiploc_route))\
            .all() }
    return [tiploc_to_crs_map[tiploc] for tiploc in tiploc_route]

def tiploc_to_crs(db: Session, tiploc: str) -> str:
    return tiploc_route_to_crs_route(db, [tiploc])[0]

@dataclass
class TrainRouteSegment:
    path: TrainPath
    start_location: str
    stop_location: str

    def __hash__(self) -> int:
        return (
            hash(self.path) ^
            hash(self.start_location) ^
            hash(self.stop_location))

TrainPath = tuple[str, ...]
TrainRoute = list[TrainRouteSegment]

