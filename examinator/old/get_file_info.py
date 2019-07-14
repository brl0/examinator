#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""get_file_info"""

import sys
import os
import inspect
from hashlib import md5
from pathlib import Path
from functools import partial
import datetime as dt

import numpy as np
import pandas as pd
import attr
from toolz import curry

from dask import compute, delayed
import dask.threaded
import dask.multiprocessing

bllb_path = str(Path(r"../../../code/python/bllb").resolve())
sys.path.insert(0, bllb_path)
from bllb_logging import *

from bllb import pp
from pprint import pprint  #as pp

def start_log(enable=True, lvl='WARNING'):
    log = setup_logging(enable, lvl, std_lib=True)
    log.info('examinator logging started')
    return log


@attr.s
class daskerator(object):
    _DSCH = {
            'd': 'distributed',
            't': 'threads',
            'p': 'processes',
            's': 'synchronous'
    }
    def _get_sched(mp_type) -> str:
        if mp_type in daskerator._DSCH.keys():
            return daskerator._DSCH[mp_type]
        else:
            return mp_type
    mp_type = attr.ib(default='s', type=str, converter=_get_sched, 
                      validator=attr.validators.in_(list(_DSCH.keys()) + list(_DSCH.values())))
    sch_add = attr.ib(default='', type=str)
    @sch_add.validator
    def check_dask_opts(instance, attribute, value):
        if instance.mp_type != 'distributed' and value != '':
            raise ValueError('Only distributed dask can accept scheduler address.')
    _client = attr.ib(default=None)
    def __attrs_post_init__(self):
        if self.mp_type[0] == 'd':
            from dask.distributed import Client
            dbg("Creating distributed client object.")
            if self.sch_add == '':
                dbg("Creating new cluster on localhost.")
                self.client = Client()
            else:
                dbg(f"Existing scheduler address: {self.sch_add}")
                self.client = Client(self.sch_add)
            log.info(self.client)
    @curry
    def run_dask(self, func, iterator):
        dbg(f'Scheduler: {self.mp_type}')
        return compute(*map(delayed(func), iterator),
                       scheduler=self.mp_type)


def get_stat(path, opt_md5=True, opt_pid=False) -> dict:
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
    if opt_md5: 
        if not path.is_dir():
            try:
                d['md5'] = md5_blocks(path)
            except:
                log.warning(f'Could not hash item: {str(path)}')
                pass
        else:
            dbg(f'Item is a directory and will not be hashed.  {str(path)}')
    if opt_pid:
        dbg(f"working using OS pid: {os.getpid()}, opt_pid: {opt_pid}")
    return d


def path_stat(path, opt_md5=True) -> pd.DataFrame:
    get_stat2 = lambda path: get_stat(path, opt_md5)
    return pd.DataFrame([*map(get_stat2, Path(path).rglob('*'))])


def path_stat_dask(dsk, path, opt_md5=True, opt_pid=False):
    get_stat2 = lambda path: get_stat(path, opt_md5, opt_pid)
    return pd.DataFrame(dsk.run_dask(get_stat2, Path(path).rglob('*')))


def md5_blocks(path, blocksize=1024 * 2048) -> str:
    path = Path(path)
    if not path.is_dir():
        try:
            hasher = md5()
            with path.open('rb') as file:
                block = file.read(blocksize)
                while len(block) > 0:
                    hasher.update(block)
                    block = file.read(blocksize)
            return hasher.hexdigest()
        except Exception as error:
            log.warning(f'Error trying to hash item: {str(path)}\nError:\n{error}')
            return
    else:
        dbg(f'Item is a directory and will not be hashed.  {str(path)}')
        return


def proc_paths(basepaths, mp_type='s', opt_md5=True):
    dsk = daskerator(mp_type)
    path_stat_dask2 = lambda path: path_stat_dask(dsk, path, opt_md5=True)
    df = pd.concat([*map(path_stat_dask2, basepaths)])
    if len(df):
        return df
    else:
        log.info('No results.')
        return
