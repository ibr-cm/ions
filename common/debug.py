from .globals import debug

def debug_print(*args):
    if debug:
        print(*args)
