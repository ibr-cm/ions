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

def setup_logging_defaults():
    # logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

