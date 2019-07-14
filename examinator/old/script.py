#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Console script for examinator."""

import sys
from examinator import start_proc, start_log

import asyncio

log = start_log("INFO", mp=True)

basepaths = ['.']

async def main():
    """Non-interactive script for examinator."""
    import time
    s = time.perf_counter()

    time.sleep(0.05)

    start_proc(basepaths)
    await asyncio.sleep(1)

    elapsed = time.perf_counter() - s
    log.info(f"{__file__} executed in {elapsed:0.2f} seconds.")
    print('\n\nFIN\n\n')

    return 0


if __name__ == "__main__":
    #sys.exit(main())  # pragma: no cover
    asyncio.run(main())
    #asyncio.run_coroutine_threadsafe(main())
