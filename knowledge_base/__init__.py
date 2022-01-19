from __future__ import annotations
from dataclasses import dataclass
from itertools import groupby
from typing import Any, Callable, Iterable, Protocol, TypeVar

class _SupportsLessThan(Protocol):
    def __lt__(self, __other: Any) -> bool: ...

_SupportsLessThanT = TypeVar("_SupportsLessThanT", bound=_SupportsLessThan)
_T = TypeVar('_T')
def group(it: Iterable[_T], key: Callable[[_T], _SupportsLessThanT]) -> dict[_SupportsLessThanT, list[_T]]:
    return { k: list(g) for k, g in groupby(sorted(it, key=key), key=key) }

@dataclass
class TrainRouteSegment:
    path: TrainPath
    start_location: str
    stop_location: str

TrainPath = tuple[str, ...]
TrainRoute = list[TrainRouteSegment]

