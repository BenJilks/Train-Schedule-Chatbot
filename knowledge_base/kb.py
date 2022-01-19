from queue import Queue
from typing import Iterable
from concurrent.futures import Executor, Future
from knowledge_base.feeds import Base, Feed, RecordSet
from knowledge_base.progress import Progress

class KBIncidents(Feed):
    def associated_tables(self) -> Iterable[type[Base]]:
        return []

    def file_name(self) -> str:
        return 'INCIDENTS.XML'

    def expiry_length(self) -> int:
        return 60 * 60 * 24 # 1 Day

    def feed_api_url(self) -> str:
        return '5.0/incidents'

    def records_in_feed(self,
                        executor: Executor,
                        chunk_queue: Queue[RecordSet | None],
                        path: str,
                        progress: Progress) -> Iterable[Future]:
        return []

Feed.register(KBIncidents)

