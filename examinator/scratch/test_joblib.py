import sys
import os
import time
from pathlib import Path
from examinator import *

from multiprocessing import Queue
from joblib import Parallel, delayed
import asyncio

WORKERS = 8
LOGURU_ENQ = True
LOG_ON = True
LOG_LEVEL = "DEBUG"

basepaths = ['..']
basepaths = map(Path, basepaths)
file_q = Queue()

def joblib_proc(path):
    if path.is_dir():
        [*map(file_q.put, filter(Path.is_file, path.iterdir()))]
        [*map(joblib_proc, filter(Path.is_dir, path.iterdir()))]
    elif path.is_file():
        file_q.put(path)
    else:
        dbg(f"Item is not a file or dir: {str(path)}")

def get_item():
    while not file_q.empty():
        yield file_q.get()

def proc_item(item):
    log.debug(f"pid: {os.getpid()}")
    log.debug(f"Processing item: {str(item)}")
    result = asyncio.run(proc_file(item))
    log.debug(f"Result: {result}")
    return result

def main():
    s = time.perf_counter()

    [*map(joblib_proc, basepaths)]
    results = Parallel(n_jobs=WORKERS, require='sharedmem')(map(delayed(proc_item), get_item()))
    pp(results)

    elapsed = time.perf_counter() - s
    log.info(f"{__file__} executed in {elapsed:0.2f} seconds.".format())
    dbg('\n\nFIN\n\n')
    return 0

if __name__ == "__main__":
    log = start_log(LOG_ON, LOG_LEVEL, mp=LOGURU_ENQ)
    dbg = log.debug
    sys.exit(main())  # pragma: no cover
