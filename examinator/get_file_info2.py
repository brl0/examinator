#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""get_file_info"""

import sys
import os
import inspect
from hashlib import md5
from pathlib import Path
from functools import partial
from operator import methodcaller
import datetime as dt

import numpy as np
import attr
from toolz import curry
from tqdm import tqdm

import pandas as pd
import dask.dataframe as dd
import dask.bag as db

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

    mp_type = attr.ib(
        default='s',
        type=str,
        converter=_get_sched,
        validator=attr.validators.in_(
            list(_DSCH.keys()) + list(_DSCH.values())))
    sch_add = attr.ib(default='', type=str)

    @sch_add.validator
    def check_dask_opts(instance, attribute, value):
        if instance.mp_type != 'distributed' and value != '':
            raise ValueError(
                'Only distributed dask can accept scheduler address.')

    _client = attr.ib(default=None)
    _cluster = attr.ib(default=None)

    def __attrs_post_init__(self):
        if self.mp_type[0] == 'd':
            from dask.distributed import Client, LocalCluster
            dbg("Creating distributed client object.")
            if self.sch_add == '':
                dbg("Creating new cluster on localhost.")
                self._cluster = LocalCluster()
                self._client = Client(self._cluster)
            else:
                dbg(f"Existing scheduler address: {self.sch_add}")
                self._client = Client(self.sch_add)
            log.info(self._client)

    @curry
    def run_dask(self, func, iterator):
        dbg(f'Scheduler: {self.mp_type}')
        if self.mp_type[0] == 'd':
            dbg('Using dask client')
            return self._client.gather(
                self._client.map(func, iterator))
        else:
            dbg('Not using dask client.')
            return compute(
                *map(delayed(func), iterator), scheduler=self.mp_type)


def get_stat(path, opt_md5=True, opt_pid=False) -> dict:
    path = Path(path)
    info = dict([
        _ for _ in inspect.getmembers(path.lstat())
        if not _[0].startswith('_') and not inspect.isbuiltin(_[1])
    ])
    info.update(
        dict([
            _ for _ in inspect.getmembers(path)
            if '__' not in _[0] and '<' not in str(_[1])
        ]))
    info.update(
        dict([(_[0], methodcaller(_[0])(path))
              for _ in inspect.getmembers(path)
              if _[0].startswith('is_') and _[0] != 'is_mount']))
    info['path'] = path
    info['f_atime'] = dt.datetime.fromtimestamp(info['st_atime'])
    info['f_ctime'] = dt.datetime.fromtimestamp(info['st_ctime'])
    info['f_mtime'] = dt.datetime.fromtimestamp(info['st_mtime'])
    if opt_md5:
        if not path.is_dir():
            try:
                md5_hash = md5_blocks(path)
                info['md5'] = md5_hash
            except:
                log.warning(f'Could not hash item: {str(path)}')
        else:
            dbg(f'Item is a directory and will not be hashed.  {str(path)}')
    if opt_pid:
        dbg(f"working using OS pid: {os.getpid()}, opt_pid: {opt_pid}")
    return info


def path_stat(path, opt_md5=True) -> pd.DataFrame:
    get_stat2 = lambda path: get_stat(path, opt_md5)
    return pd.DataFrame([*map(get_stat2, Path(path).rglob('*'))])


def path_stat_dask(dsk, path, opt_md5=True, opt_pid=False):
    path = Path(path)
    get_stat2 = lambda path: get_stat(path, opt_md5, opt_pid)
    if path.is_dir():
        return dsk.run_dask(get_stat2, tqdm(path.rglob('*')))
    else:
        return dsk.run_dask(get_stat2, [path])


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
            log.warning(
                f'Error trying to hash item: {str(path)}\nError:\n{error}')
            return
    else:
        dbg(f'Item is a directory and will not be hashed.  {str(path)}')
        return


def proc_paths(basepaths, mp_type='s', opt_md5=True):
    dsk = daskerator(mp_type)
    path_stat_dask2 = lambda path: path_stat_dask(dsk, path, opt_md5=opt_md5)
    result = [*map(path_stat_dask2, basepaths)]
    dbg(str(type(result)))
    if len(result):
        return result
    else:
        log.info('No results.')
        return
