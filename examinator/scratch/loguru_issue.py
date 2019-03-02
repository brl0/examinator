import sys
import os
import time
from loguru import logger
from concurrent.futures import ProcessPoolExecutor as executor
#import asyncio

enqueue = True
level = "DEBUG"
WORKERS = 20
loops = 1000

def worker(w, loops):
    logger.debug(f"Starting worker: {w}, pid: {os.getpid()}")
    for i in range(loops):
        for j in range(i):
            k = i ** 2 + j ** 2
        logger.debug(f"Worker: {w}, Loop {i+1} of {loops}")

def main():
    s = time.perf_counter()

    with executor(max_workers=WORKERS) as pool:
        for w in range(WORKERS):
            logger.debug(f"Submitting worker {w}")
            pool.submit(worker, w, loops)
        logger.debug("Finished submitting workers.")
    logger.debug("Finished with executor.")

    elapsed = time.perf_counter() - s
    logger.info(f"{__file__} executed in {elapsed:0.2f} seconds.".format())

if __name__ == "__main__":
    logger.remove()
    logger.add(sys.stdout, level=level, enqueue=enqueue)
    logger.enable("loguru")
    #sys.exit(asyncio.run(main()))  # pragma: no cover
    sys.exit(main())  # pragma: no cover
