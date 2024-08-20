def start_ipython_dbg_cmdline(user_ns=None):
    from IPython import start_ipython
    if user_ns:
        start_ipython(argv=[], user_ns=user_ns)
    else:
        start_ipython(argv=[])

def start_debug(frame=None):
    r"""
    Start an interactive ipdb debugging session in the given frame context.
    If no frame is provided, use the caller's frame.
    For information on frame objects see `inspect <https://docs.python.org/3/library/inspect.html>`_.

    Parameters
    ----------
    frame: Optional[frame]
        The frame context of interest.
    """
    import ipdb
    if not frame:
        # get the frame of the caller
        import inspect
        frame = inspect.currentframe().f_back
    ipdb.set_trace(frame=frame)
