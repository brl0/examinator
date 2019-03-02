import sys
import time
from random import random
from pathlib import Path
from examinator import *
import asyncio
import concurrent.futures

import itertools
from collections import Counter

#from joblib import Parallel, delayed

basepaths = ['.']
basepaths = map(Path, basepaths)

counter = Counter()
def cc(s):
    counter[s] += 1
    return counter[s]
def ccd(s, i):
    counter[s] += i
    return counter[s]

#@asyncio.coroutine
async def proc_path(path, sleep=0):
    i = cc('new_id')
    dbg(f'proc_path: {i}, {sleep}, {path}')
    await asyncio.sleep(sleep)
    count_down = ccd('out_id', i)
    print(f'{i} {count_down}\t| {str(path)}')
    if path.is_dir():
        return [asyncio.run_coroutine_threadsafe(
            proc_path(_, random()/(10*sleep+1)), 
            asyncio.get_running_loop())
        for _ in path.iterdir()]

async def waiter(futures):
    for f in futures:
        if 'Future' in str(type(f)):
            while not f.done():
                dbg(f'{cc("wait")} waiting on Future')
                await asyncio.sleep(.01)
            dbg(f'result type: {str(type(f.result()))}')
            if type(f.result()) == list:
                dbg(f'result type: {str(type(f.result()[0]))}')
                await waiter(f.result())
        else:
            log.warning(f'waiter found type: {str(type(f))}')
            await waiter(f)

async def main():
    s = time.perf_counter()

    loop = asyncio.get_running_loop()

    procs = []
    for path in basepaths:
        dbg(f'start processing: {path}')
        procs.append(asyncio.run_coroutine_threadsafe(proc_path(path), loop))
    dbg(f'{str(type(procs))}')
    for f in procs:
        dbg(f'{str(type(f))}')
    #for f in concurrent.futures.as_completed(procs):
        #dbg(f'future as_completed')
    for f in procs:
        dbg(type(f))
        while not f.done():
            dbg(f'{cc("wait")} waiting on procs')
            await asyncio.sleep(.01)
        await waiter(f.result())
    dbg('basepath proc_path done')

    elapsed = time.perf_counter() - s
    log.info(f"{__file__} executed in {elapsed:0.2f} seconds.")
    print('\n\nFIN\n\n')
    return 0

if __name__ == "__main__":
    log = start_log("DEBUG", mp=True)
    sys.exit(asyncio.run(main()))  # pragma: no cover
