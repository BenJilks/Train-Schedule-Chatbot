import sys
from dataclasses import dataclass
from queue import Queue
from threading import Lock
from typing import Union

BAR_LENGTH = 50
NAME_LEN = 20

@dataclass
class ProgressBar:
    done: int
    out_of: int

class Progress:
    _bars: dict[str, ProgressBar]
    _last_bar_count: Union[int, None]
    _mutex: Lock
    _report_queue: Queue[tuple[str, int, int]]

    def __init__(self):
        self._bars = {}
        self._last_bar_count = None
        self._mutex = Lock()
        self._report_queue = Queue()

    def _draw_bar(self, bar: ProgressBar):
        progress = int((float(bar.done) / float(bar.out_of)) * BAR_LENGTH)
        print(''.join([
            ('>' if i == progress else '=') if i <= progress else ' '
            for i in range(BAR_LENGTH)
        ]), end='')
        print(f'] { bar.done } / { bar.out_of }', end='')

    def _clean_draw(self):
        if len(self._bars) == 0:
            print()
            return

        self._last_bar_count = len(self._bars)
        for name, bar in self._bars.items():
            display_name = name if len(name) < NAME_LEN else (name[:(NAME_LEN - 3)] + '...')
            print(('{:>' + str(NAME_LEN) + '} [').format(display_name), end='')
            self._draw_bar(bar)
            print()

    def _quick_update(self):
        print(f"\033[G\033[{ self._last_bar_count }A", end='')
        for bar in self._bars.values():
            print(f"\033[{ NAME_LEN + 2 }C\033[K", end='')
            self._draw_bar(bar)
            print('\033[E', end='')
        print('\033[G', end='')
        sys.stdout.flush()

    def _update_report(self, name: str, done: int, out_of: int):
        if done >= out_of:
            if name in self._bars:
                del self._bars[name]
        else:
            self._bars[name] = ProgressBar(done + 1, out_of)

    def display(self):
        if len(self._bars) == self._last_bar_count:
            self._quick_update()
            return

        if not self._last_bar_count is None:
            print(f"\033[G\033[{ self._last_bar_count }A\033[J", end='')
        self._clean_draw()
        return

    def report(self, name: str, done: int, out_of: int):
        # If we don't get the lock, queue it up for later
        if not self._mutex.acquire(blocking=False):
            self._report_queue.put((name, done, out_of))
            return
        
        # Report anything in the queue first
        while not self._report_queue.empty():
            self._update_report(*self._report_queue.get())

        self._update_report(name, done, out_of)

        self.display()
        self._mutex.release()

