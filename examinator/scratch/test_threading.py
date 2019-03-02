import sys
import time
from pathlib import Path
from queue import Queue
from multiprocessing.dummy import Pool
import threading
import asyncio
from examinator import *

log = setup_logging()
log.debug('test')
q = Queue()
num_worker_threads = 4
basepaths = ['..']

def worker():
    while True:
        item = q.get()
        if item is None:
            break
        if item.is_file():
            pp(asyncio.run(proc_file(item)))
        elif item.is_dir():
            [*map(q.put, item.iterdir())]
        q.task_done()

async def main():
    s = time.perf_counter()
    threads = []
    for i in range(num_worker_threads):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)

    for item in map(Path, basepaths):
        q.put(item)

    # block until all tasks are done
    q.join()

    # stop workers
    for i in range(num_worker_threads):
        q.put(None)
    for t in threads:
        t.join()

    elapsed = time.perf_counter() - s
    log.info(f"{__file__} executed in {elapsed:0.2f} seconds.")
    print('\n\nFIN\n\n')
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))  # pragma: no cover
