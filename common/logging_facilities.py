import logging

# ---

def log(msg, level=logging.INFO, *args):
    logging.log(msg=msg, level=level, *args)

def logd(msg, *args):
    logging.log(msg=msg, level=logging.DEBUG, *args)

def logw(msg, *args):
    logging.log(msg=msg, level=logging.WARN, *args)

def loge(msg, *args):
    logging.log(msg='-'*40, level=logging.ERROR, *args)
    logging.log(msg=msg, level=logging.ERROR, *args)
    logging.log(msg='-'*40, level=logging.ERROR, *args)

def logi(msg, *args):
    logging.log(msg=msg, level=logging.INFO, *args)

# ---

def setup_logging_defaults(level=logging.WARNING, silence_font_manager:bool=True):
    logging.basicConfig(format='%(levelname)s | %(module)s::%(name)s::%(funcName)s | %(message)s', level=level)
    if silence_font_manager:
        logging.getLogger('matplotlib.font_manager').setLevel(logging.WARNING)

def set_logging_level(level=logging.WARNING):
    logging.getLogger().setLevel(level)
    logi(f'logging level set to {logging.getLevelName(level)}')

