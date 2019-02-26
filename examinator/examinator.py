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

#from multiprocessing import Pool
#import asyncio

from multiprocessing.dummy import Pool
from queue import Queue

q = Queue()

def start_log(lvl='WARNING', mp=False):
    log = setup_logging(True, lvl, loguru_enqueue=mp)
    log.info('examinator logging started')
    return log


def file_as_blockiter(path, blocksize=65536):
    with open(path, 'rb') as file:
        block = file.read(blocksize)
        while len(block) > 0:
            yield block
            block = file.read(blocksize)

#async 
def get_md5(path):
    hasher = md5()
    for block in file_as_blockiter(path):
        hasher.update(block)
    dbg(f'\nhasher: {hasher.hexdigest()} {path.resolve()}')
    return hasher.hexdigest()

#async 
def proc_file(path):
    dbg(f'proc_file: {path.resolve()}')
    return {str(path): get_md5(path)}

def proc(path):
    dbg(f'{str(path.resolve())}')
    if path.is_file():
        dbg('call proc file')
        return proc_file(path)
        #return asyncio.run(proc_file(path))
    elif path.is_dir():
        dbg('proc dir')
        #with Pool() as pool:
        return {str(path.resolve()): [*map(proc, path.iterdir())]}
    else:
        log.warning(f'not a file or dir: {str(path)}')


def start_proc(basepaths):
    #pool = Pool()
    for path in basepaths:
        pp({path: proc(path)})
    