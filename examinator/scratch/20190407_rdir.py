#%%
import os


def rdir(path):
    if os.path.isdir(path):
        return {path: [rdir(p) if p.is_dir() else p for p in os.scandir(path)]}
    return [path]


rdir('.')
