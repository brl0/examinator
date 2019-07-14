# -*- coding: utf-8 -*-
"""Main module."""

import sys
from pathlib import Path
bllb_path = str(Path(r"../../../code/python/bllb").resolve())
sys.path.insert(0, bllb_path)
from bllb import *

import os
from random import random
from hashlib import md5
import functools
from pprint import pformat

import asyncio
import concurrent.futures

WORKERS = 4

from multiprocessing import Value, Array, Lock
slept = Value('i', 0)
starts = Value('i', 0)
results_count = Value('i', 0)
result_loop = Value('i', 0)
worker_count = Value('i', 0)
lock = Lock()

from multiprocessing import Queue as pQueue
from queue import Queue as tQueue

holder = {
    'mp_type': 'p',
    'p': {
        'Ex': concurrent.futures.ProcessPoolExecutor,
        'q': pQueue(),
        'r': pQueue()
    },
    't': {
        'Ex': concurrent.futures.ThreadPoolExecutor,
        'q': tQueue(),
        'r': tQueue()
    }
}

def start_log(enable=True, lvl='WARNING', mp=True):
    log = setup_logging(enable, lvl, loguru_enqueue=mp)
    log.info('examinator logging started')
    return log

def file_as_blockiter(path, blocksize=65536):
    with open(path, 'rb') as file:
        block = file.read(blocksize)
        while len(block) > 0:
            yield block
            block = file.read(blocksize)

def get_md5(path):
    hasher = md5()
    for block in file_as_blockiter(path):
        hasher.update(block)
    dbg(f'\nhasher: {hasher.hexdigest()} {path.resolve()}')
    return hasher.hexdigest()

def proc_file(path):
    dbg(f'proc_file: {path.resolve()}')
    return {str(path): get_md5(path)}

def proc_paths(basepaths, mp_type='p'):
    holder['mp_type'] = mp_type
    dbg(f'mp_type: {mp_type}')
    dbg(
        f'holder["mp_type"]: {holder["mp_type"]}'
    )
    dbg(f'holder\n{pformat(holder)}')
    poolExecutor = holder[mp_type]['Ex']
    q = holder[mp_type]['q']
    r = holder[mp_type]['r']
    dbg(f"q type: {str(type(q))}")
    dbg(f'count basepaths: {len(basepaths)}')
    [*map(q.put, map(Path, basepaths))]
    dbg(f'q size: {q.qsize()}')

    with poolExecutor(WORKERS) as pool:
        dbg('got pool exec')
        for i in range(WORKERS):
            pool.submit(worker)
        dbg('\npool submit done\n')
        while not q.empty() or worker_count.value > 0:
            dbg(f'Still processing. q.empty: {q.empty()}, worker_count: {worker_count.value}')
            asyncio.run(sleeper(.1))
        pool.shutdown()

    dbg('\n\nResults:\n\n')
    while not r.empty() or not q.empty() or worker_count.value > 0:
        with lock:
            result_loop.value += 1
        dbg(f'\nResult loop: {result_loop.value}')
        if not r.empty():
            result = r.get()
            with lock:
                results_count.value += 1
            dbg(
                f'result count: {results_count.value}'
            )
            pp(result)
        else:
            asyncio.run(sleeper(.1))
    #pool.shutdown(True)
    dbg('proc_paths complete')

async def sleeper(seconds):
    with lock:
        slept.value += 1
    dbg(
        f'sleep count: {slept.value}'
    )
    await asyncio.sleep(seconds)

def worker():
    asyncio.run(sleeper(.1))
    with lock:
        worker_count.value += 1
        pid = worker_count.value
    dbg(f'worker_count: {worker_count.value}')
    dbg(
        f'holder["mp_type"]: {holder["mp_type"]}'
    )
    
    mp_type = holder['mp_type']
    q = holder[mp_type]['q']
    r = holder[mp_type]['r']
    units = 0

    dbg(f'\nstarting worker: {pid} {os.getpid()}\n')
    while q.empty() and starts.value < 20:
        with lock:
            starts.value += 1
        log.warning(
            f'worker {pid} {os.getpid()}, q is empty, restarts: {starts.value}'
        )
        asyncio.run(sleeper(.1))

    if starts.value >= 20:
        log.warning(f"problem starting worker: {pid} {os.getpid()}")
    else:
        dbg(f'start worker count: {starts.value}')

    while not q.empty():
        item = q.get()
        #dbg(f'worker {pid} loop processing item: {units} {str(item)}')
        if item.is_file():
            result = proc_file(item)
            #dbg(result)
            r.put(result)
        elif item.is_dir():
            [*map(q.put, item.iterdir())]
        else:
            dbg(f'{pid} not a file or dir: {str(item)}')
        units += 1
    dbg(f'\n\nworker finished: {pid}\tunits: {units}\n\n')
    with lock:
        worker_count.value -= 1
    dbg(f'worker exit, remaining worker_count: {worker_count.value}')
