import sys
from pathlib import Path
from examinator import *

log = setup_logging()

log.debug('test')

from queue import Queue
from multiprocessing.dummy import Pool
q = Queue()

q.put(Path('..'))

with Pool() as pool:
    while not q.empty():
        item = q.get()
        if item.is_dir():
            pool.map(q.put, item.iterdir())
        elif item.is_file():
            print(str(item))

