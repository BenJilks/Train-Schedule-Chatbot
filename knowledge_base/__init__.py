from __future__ import annotations
from dataclasses import dataclass

@dataclass
class TrainRouteSegment:
    path: TrainPath
    start_location: str
    stop_location: str

TrainPath = tuple[str, ...]
TrainRoute = list[TrainRouteSegment]

