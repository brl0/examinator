#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Console script for examinator."""

import sys
from pathlib import Path
from operator import truth
import click
from get_file_info2 import proc_paths, start_log, pp

LOG_ON = False
LOG_LEVEL = "WARNING"


@click.command()
@click.option(
    '-b',
    '--basepaths',
    default='.',
    help='Base path.',
    multiple=True,
    type=click.Path(exists=True))
@click.option(
    '-f', '--file', help='File path or - for stdin', type=click.File('r'))
@click.option(
    '-m',
    '--mp',
    default='s',
    show_default=True,
    help='Multiprocessing type, p: process, t: threading, d: distributed, s: synchronous.',
    type=click.Choice(['p', 't', 'd', 's']))
@click.option('--md5/--no-md5', default=True)
@click.option('-v', '--verbose', count=True)
@click.argument('args', nargs=-1)
def main(basepaths, file, mp, md5, verbose, args):
    """Console script for examinator."""
    # click documentation at http://click.pocoo.org/
    import time
    s = time.perf_counter()
    log_on = LOG_ON
    log_level = LOG_LEVEL

    if verbose:
        print('verbose')
        log_on = True
        log_level = max(4 - verbose, 1) * 10
    log = start_log(log_on, log_level)
    log.warning(f"\nlogs enabled: {log_on}\nlog_level: {log_level}")
    log.debug("basepaths: {}".format(basepaths))
    log.debug('{}'.format(str(type(file))))
    log.debug(f'Calculate md5 hash: {md5}')
    log.debug('{}'.format(args))
    time.sleep(0.05)  # Sleep to let logging initiate

    if not basepaths:
        basepaths = []
    else:
        basepaths = list(basepaths)
    log.debug(f"{str(type(basepaths))}")
    if file:
        basepaths += file.read().split('\n')
    log.debug(f"\n{str(type(basepaths))}\n{basepaths}\n")
    df = proc_paths(basepaths, mp, opt_md5=md5)
    try:
        pp(df.info())
        fields = ['path', 'st_size']
        if md5:
            fields.append('md5')
        pp(df.loc[:, fields])
    except:
        print('No results')

    elapsed = time.perf_counter() - s
    print(f"{__file__} executed in {elapsed:0.2f} seconds.".format())
    print('\n\nFIN\n\n')

    return 0


if __name__ == "__main__":

    sys.exit(main())  # pragma: no cover
