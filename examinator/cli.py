#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Console script for examinator."""

import sys
import click
from examinator import start_proc, start_log

log = start_log("DEBUG")


@click.command()
@click.option(
    '-b',
    '--basepaths',
    default='.',
    help='Base path.',
    multiple=True,
    type=click.Path(exists=True))
@click.option('-f', '--file', help='File path.', type=click.File('r'))
@click.argument('a', nargs=-1)
def main(basepaths, file, a):
    """Console script for examinator."""
    # click documentation at http://click.pocoo.org/
    import time
    s = time.perf_counter()

    log.debug(f'{basepaths}')
    log.debug(f'{file}')
    log.debug(f'{a}')
    time.sleep(0.05)

    if not basepaths:
        basepaths = []
    if file:
        basepaths.append(file.read().split('\n'))
    start_proc(basepaths)

    elapsed = time.perf_counter() - s
    log.info(f"{__file__} executed in {elapsed:0.2f} seconds.")
    print('\n\nFIN\n\n')

    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
