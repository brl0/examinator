import sys
import time
from pathlib import Path
from queue import Queue
from multiprocessing.dummy import Pool
import asyncio
from examinator import *

log = setup_logging()
log.debug('test')
q = Queue()

async def main():
    s = time.perf_counter()
    with Pool() as pool:
        while not q.empty():
            item = q.get()
            if item.is_dir():
                pool.map(q.put, item.iterdir())
            elif item.is_file():
                print(await proc_file(item))
    elapsed = time.perf_counter() - s
    log.info(f"{__file__} executed in {elapsed:0.2f} seconds.")
    print('\n\nFIN\n\n')
    return 0

if __name__ == "__main__":
    q.put(Path('..'))
    sys.exit(asyncio.run(main()))  # pragma: no cover
