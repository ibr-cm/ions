import logging

from logging import debug as logd
from logging import info as logi
from logging import warning as logw
from logging import error as loge

# ---

def setup_logging_defaults(level=logging.WARNING, silence_font_manager:bool=True, silence_ipython_parser:bool=True):
    logging.basicConfig(format='%(levelname)s | %(module)s::%(name)s::%(funcName)s | %(message)s', level=level)
    if silence_font_manager:
        logging.getLogger('matplotlib.font_manager').setLevel(logging.WARNING)
    if silence_ipython_parser:
        logging.getLogger('parso.python.diff').setLevel(logging.WARNING)
        logging.getLogger('parso.cache').setLevel(logging.WARNING)

def set_logging_level(level=logging.WARNING):
    logging.getLogger().setLevel(level)
    logi(f'logging level set to {logging.getLevelName(level)}')

