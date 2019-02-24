# -*- coding: utf-8 -*-
"""Main module."""

import sys
from pathlib import Path
bllb_path = str(
    Path(r"../../../code/python/bllb").resolve())
sys.path.insert(0, bllb_path)
from bllb import *

import os
from random import random
from hashlib import md5

#from multiprocessing.dummy import Pool as ThreadPool

#pool = ThreadPool(1)


def start_log(lvl='WARNING'):
    log = setup_logging(True, lvl)
    log.info('examinator logging started')
    return log


def file_as_blockiter(path, blocksize=65536):
    with open(path, 'rb') as file:
        block = file.read(blocksize)
        while len(block) > 0:
            yield block
            block = file.read(blocksize)


def get_md5(file):
    hasher = md5()
    for block in file_as_blockiter(file):
        hasher.update(block)
    return hasher.hexdigest()


def proc_items(entry, depth=0):
    path = Path(entry)
    log.debug(path.resolve())
    if path.is_dir():
        log.debug(f'Processing: {depth} ({path.parent.name})| {path}')
        proc_child = lambda child: proc_items(child, depth + 1)
        return list(map(proc_child, os.scandir(path)))
    else:
        return {str(path): get_md5(path)}


dbg = log.debug


def p2(it):
    dbg(type(it))
    if type(it) == str:
        print(it)
        return
    try:
        for key, value in it.items():
            print(key, value)
        dbg('iteritems')
    except Exception as error:
        dbg(error)
        try:
            for _ in it:
                p2(_)
        except Exception as error:
            dbg(error)
            print(it)


def start_proc(basepaths):
    p(map(proc_items, basepaths))