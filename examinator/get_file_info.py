import sys
import os
import inspect
from hashlib import md5
from pathlib import Path
from pprint import pprint
from functools import partial
import datetime as dt

import numpy as np
import pandas as pd

bllb_path = str(Path(r"../../../code/python/bllb").resolve())
sys.path.insert(0, bllb_path)
from bllb import *  #pp, setup_logging

from dask.distributed import Client
from dask import compute, delayed
import dask.threaded, dask.multiprocessing
DSCH = {
    'd': 'distributed',
    't': 'threads',
    'p': 'processes',
    's': 'synchronous'
}


def start_log(enable=True, lvl='WARNING', mp=True):
    log = setup_logging(enable, lvl, loguru_enqueue=mp)
    log.info('examinator logging started')
    return log


def get_stat(path) -> dict:
    path = Path(path)
    info = path.lstat()
    d = dict([
        t for t in inspect.getmembers(info)
        if not t[0].startswith('_') and not inspect.isbuiltin(t[1])
    ])
    d['path'] = path
    d['f_atime'] = dt.datetime.fromtimestamp(d['st_atime'])
    d['f_ctime'] = dt.datetime.fromtimestamp(d['st_ctime'])
    d['f_mtime'] = dt.datetime.fromtimestamp(d['st_mtime'])
    if path.is_file():
        d['md5'] = md5_blocks(path)
    dbg(f"OS pid: {os.getpid()}")
    return d


def md5_blocks(path, blocksize=1024 * 2048) -> str:
    path = Path(path)
    if path.is_file():
        try:
            hasher = md5()
            with path.open('rb') as file:
                block = file.read(blocksize)
                while len(block) > 0:
                    hasher.update(block)
                    block = file.read(blocksize)
            return hasher.hexdigest()
        except:
            return
    else:
        return


def path_stat(path) -> pd.DataFrame:
    return pd.DataFrame([*map(get_stat, Path(path).rglob('*'))])


def path_stat_dask(scheduler, path) -> pd.DataFrame:
    log.debug(scheduler)
    return pd.DataFrame(
        compute(
            *map(delayed(get_stat),
                 Path(path).rglob('*')),
            scheduler=scheduler))


def proc_paths(basepaths, mp_type='s'):
    if mp_type == 'd':
        client = Client()
    mp_type = DSCH[mp_type]
    path_stat_d = partial(path_stat_dask, mp_type)
    df = pd.concat([*map(path_stat_d, basepaths)])
    if len(df):
        pp(df.loc[:, ['path', 'md5', 'st_size']])
        dbg(df.info())
    else:
        print('No results.')
