import sys
from pathlib import Path
bllb_path = str(
    Path(r"C:\Users\b_r_l\OneDrive\Documents\code\python\bllb\\").resolve())
sys.path.insert(0, bllb_path)
from bllb import *

print('imported bllb')
print_sysinfo()

p(sys.path)

from examinator import *

log = setup_logging()

log.debug('test')
