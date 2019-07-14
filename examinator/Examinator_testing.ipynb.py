#%% Change working directory from the workspace root to the ipynb file location. Turn this addition off with the DataScience.changeDirOnImportExport setting
import os
try:
	os.chdir(os.path.join(os.getcwd(), 'examinator'))
	print(os.getcwd())
except:
	pass

#%%
import sys
import os
import inspect
from hashlib import md5
from pathlib import Path
from operator import methodcaller
from itertools import chain
import datetime as dt
import click
import attr
from tqdm import tqdm
import numpy as np
from toolz import curry
from functools import partial
import pandas as pd
import dask.array as da
import dask.bag as db
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
from pprint import pprint  #as pp

from IPython.core.display import HTML
from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"


#%%
import binascii
import hashlib
def hash_utf8(string):
    """given utf8 string return md5 hash value as hex string"""
    hasher = hashlib.md5()
    hasher.update(string.encode("utf-8"))
    return binascii.hexlify(hasher.digest()).decode("utf-8")


#%%
import logging as log
log.disable(50)


#%%
bllb_path = str(Path(r"../../../code/python/bllb").resolve())
sys.path.insert(0, bllb_path)
from bllb_logging import *
from bllb import pp  #, hash_utf8

LOG_ON = False
LOG_LEVEL = "WARNING"  #"DEBUG"
def start_log(enable=True, lvl='WARNING', std_lib=True):
    log = setup_logging(enable, lvl, std_lib=std_lib)
    log.info('examinator logging started')
    return log
log_on = LOG_ON
log_level = LOG_LEVEL
log = start_log(log_on, log_level, std_lib=True)


#%%
#cluster = LocalCluster(processes=True)
client = Client('127.0.0.1:38615')
client


#%%
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
def glob_paths(path):
    try:
        path = Path(path)
        if path.is_dir():
            return path.rglob('*')
        else:
            return path
    except Exception as error:
        log.warning(error)


#%%
def get_stat(path, opt_md5=True, opt_pid=False) -> dict:
    log.debug(path)
    try:
        path = Path(path)
        info = dict([
            _ for _ in inspect.getmembers(path.lstat())
            if not _[0].startswith('_') and not inspect.isbuiltin(_[1])
        ])
        info.update(
            dict([
                (_[0], str(_[1])) for _ in inspect.getmembers(path)
                if '__' not in _[0] and '<' not in str(_[1])
            ]))
        info.update(
            dict([(str(_[0]), methodcaller(_[0])(path))
                  for _ in inspect.getmembers(path)
                  if _[0].startswith('is_') and _[0] != 'is_mount']))
        info['path'] = str(path)
        info['path_hash'] = hash_utf8(str(path))
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
                log.debug(f'Item is a directory and will not be hashed.  {str(path)}'
                    )
        if opt_pid:
            log.debug(f"working using OS pid: {os.getpid()}, opt_pid: {opt_pid}")
        return info
    except Exception as error:
        log.warning(error)
        return {'path': str(path)}


#%%
path = Path('.')
get_stat(path)


#%%



#%%
def flatten(lists):
    return reduce(lambda res, x: res + (flatten(x) if isinstance(x, list) else [x]), lists, [])


#%%
get_ipython().run_cell_magic('time', '', 'basepaths = [\'..\']\nopt_md5=False\n#def proc_paths(basepaths, opt_md5=True):\n"""proc_paths uses Dask client to map path_stat over basepaths."""\npaths = chain.from_iterable(map(glob_paths, basepaths))\npstat = partial(get_stat, opt_md5=opt_md5, opt_pid=True)\nresults = client.map(pstat, paths)\ndata = [_.result() for _ in results]\nddf = dd.from_pandas(pd.DataFrame(data), npartitions=4)\ndf = ddf.compute()\n#df[\'idx\'] = df.index\n#df[\'path_hash\'] = df.path.map(str).map(hash_utf8)\n#times = df.loc[:, [\'idx\', \'path\', \'f_ctime\', \'f_mtime\', \'f_atime\']].melt(id_vars=[\'idx\', \'path\'])')


#%%
path = Path('..')
basepaths = [str(path)]
def proc_item(path):
    return [*map(str, path.iterdir())] + [*map(proc_item, filter(Path.is_dir, path.iterdir()))]
result = proc_item(path)
print(len([*path.rglob('*')]))
print(len(result))
results = [*result]
print(len(results))
final = flatten(results)
print(len(final))


#%%
def flatten(lists):
    return reduce(lambda res, x: res + (flatten(x.iterdir()) if x.is_dir() else [str(x)]), lists, [])
flatten(Path('.').iterdir())


#%%
is_iter = lambda item: item.is_dir()
rfunc = lambda res, x: res + (flatten(x.iterdir()) if is_iter(x) else [str(x)])
def flatten(iterator):
    return reduce(rfunc, iterator, [])
flatten(Path('.').iterdir())


#%%
import operator
is_iter = lambda item: item.is_dir()
get_kids = lambda parent: parent.iterdir()
get_val = lambda item: flatten(get_kids(item)) if is_iter(item) else [item]
def flatten(iterator):
    results = []
    for i in iterator:
        results = partial(operator.add, results)(get_val(i))
    return results
[*flatten(Path('.').iterdir())]


#%%
def get_dir(d):
    path = Path(d)
    if path.is_dir():
        return [str(_) for _ in path.iterdir()]
get_dir('.')


#%%
from queue import Queue
from threading import Thread

def multiplex(n, q, **kwargs):
    """ Convert one queue into several equivalent Queues

    >>> q1, q2, q3 = multiplex(3, in_q)
    """
    out_queues = [Queue(**kwargs) for i in range(n)]
    def f():
        while True:
            x = q.get()
            for out_q in out_queues:
                out_q.put(x)
    t = Thread(target=f)
    t.daemon = True
    t.start()
    return out_queues

def push(in_q, out_q):
    while True:
        x = in_q.get()
        out_q.put(x)

def merge(*in_qs, **kwargs):
    """ Merge multiple queues together

    >>> out_q = merge(q1, q2, q3)
    """
    out_q = Queue(**kwargs)
    threads = [Thread(target=push, args=(q, out_q)) for q in in_qs]
    for t in threads:
        t.daemon = True
        t.start()
    return out_q


#%%
get_ipython().run_cell_magic('time', '', '\nfrom queue import Queue\nfrom threading import Thread\nfrom time import sleep\n\nq = Queue()\nremote_q = client.scatter(q)\nq1, q2 = multiplex(2, remote_q)\nlist_q = client.map(get_dir, q1)\nl_q = client.gather(list_q)\n\nopt_md5 = True\n\npstat = partial(get_stat, opt_md5=opt_md5, opt_pid=False)\nq3 = client.map(pstat, q2)\nq4, q5 = multiplex(2, q3)\nresult_q = client.gather(q4)\n\nqs = [q, remote_q, q1, q2, list_q, l_q, q3, q4, result_q]\n\ndef load_dir(from_q, to_q, stop):\n    limit = 300\n    i = limit\n    while True and ((i and not stop()) or from_q.qsize()):\n        if from_q.qsize():\n            l = from_q.get()\n            if isinstance(l, list):\n                for item in l:\n                    to_q.put(item)\n            i = min(i+1, limit)\n        else:\n            i -= 1\n            sleep(.1)\n\ndef unloadq(q, stop, limit=2000, rest=.1, check=100):\n    i = limit\n    loops = 0\n    results = []\n    while True and ((i and not stop()) or q.qsize()):\n        loops += 1\n        if loops % check == 0:\n            print(i, loops, len(results))\n        if q.qsize():\n            x = q.get()\n            #print(x)\n            results.append(x)\n            i = min(i+1, limit)\n        else:\n            i -= 1\n            if i % check == 0:\n                print(i)\n            sleep(rest)\n    return results')


#%%
get_ipython().run_cell_magic('time', '', "#load_thread = Thread(target=load_dir, args=(l_q, q,), daemon = True)\n#load_thread.start()\nfrom concurrent.futures import ThreadPoolExecutor\nbasepaths = ['.']\nwith ThreadPoolExecutor() as t:\n    stop_threads = False\n    stop = lambda: stop_threads\n    t.submit(load_dir, l_q, q, stop)\n    [q.put(str(Path(path).resolve())) for path in basepaths]\n    results_future = t.submit(unloadq, result_q, stop, limit=300)\n    ilimit = 10\n    i = ilimit\n    while i:\n        alive = sum([_q.qsize() for _q in qs])\n        if alive:\n            i = min(i+1, ilimit)\n            print(alive, i)\n        else:\n            i -= 1\n            print(f'i: {i}')\n        sleep(.1)\n    stop_threads = True\n    #results_list = unloadq(result_q, limit=300)\n    results_list = results_future.result()\n    results = pd.DataFrame(results_list)\n    print(results.info())\n    print(results.sample(5))\n#t.shutdown(False)\n#del(load_thread)\nprint(q5.qsize())")


#%%
def iterq(q):
    while q.qsize():
        yield q.get()

print(q5.qsize())
result_count = q5.qsize()
data = client.gather(q5)
while data.qsize() < result_count:
    print('sleeping')
    sleep(.1)
print(data.qsize())
iterdata = [*iterq(data)]
print(len(iterdata))
df = pd.DataFrame(iterdata)
print(len(df))
ddf = dd.from_pandas(df, npartitions=4)
remote_ddf = client.scatter(ddf)
remote_result = remote_ddf.result()
remote_result.to_csv('./export4-*.csv')
new_ddf = dd.read_csv('./export4-*.csv')
new_ddf.compute()


#%%
print(len(remote_result))


#%%



#%%
df.info()


#%%
df.to_hdf('./export7.hdf', 'key')


#%%
new_df = pd.read_hdf('./export7.hdf')


#%%
remote_result.to_hdf('./export5-*.hdf', 'key')
new_ddf = dd.read_hdf('./export5-*.hdf', 'key')
new_ddf.compute().sample(5)


#%%
new_ddf = pd.read_hdf('./export5-*.hdf', 'key')


