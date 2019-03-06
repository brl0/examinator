import inspect
from hashlib import md5
from pathlib import Path

def get_stat(path) -> dict:
    info = path.lstat()
    d = dict([t for t in inspect.getmembers(info) if not t[0].startswith('_') and not inspect.isbuiltin(t[1])])
    d['path'] = path
    d['f_atime'] = dt.datetime.fromtimestamp(d['st_atime'])
    d['f_ctime'] = dt.datetime.fromtimestamp(d['st_ctime'])
    d['f_mtime'] = dt.datetime.fromtimestamp(d['st_mtime'])
    if path.is_file():
        d['md5'] = md5_blocks(path)
    return d

def md5_blocks(path, blocksize=1024*2048) -> str:
    path = Path(path)
    if not path.is_file():
        return
    else:
        hasher = md5()
        with path.open('rb') as file:
            block = file.read(blocksize)
            while len(block) > 0:
                hasher.update(block)
                block = file.read(blocksize)
        return hasher.hexdigest()

def path_stat(path):
    return pd.DataFrame([*map(get_stat, Path(path).rglob('*'))])

def path_stat_dask(path):
    return pd.DataFrame(
        compute(
            *map(
                delayed(get_stat),
                     Path(path).rglob('*')),
                scheduler='threads'))

