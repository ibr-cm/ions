from .globals import debug

def debug_print(*args):
    if debug:
        print(*args)

def start_ipython_dbg_cmdline(user_ns=None):
    from IPython import start_ipython
    if user_ns:
        start_ipython(argv=[], user_ns=user_ns)
    else:
        start_ipython(argv=[])
