Debugging
=========

Some useful tips for debugging a recipe or the framework itself:
- when developing code for a transform or debugging errors, it can be really
  useful to start an interactive console by adding:
  ```
  start_ipython_dbg_cmdline(user_ns=locals())
  ```
  into the code in the recipe and running single-threaded by adding `--worker
  1 --single-threaded` to the command line. This also allows modifying data and
  plots on the fly and continue the normal program flow afterwards.
- alternatively, one can insert a `from common.debug import start_debug;
  start_debug()`. This will drop into a ipdb debugger session, which allows
  traversing the stack of the interpreter. The program flow can be continued
  with a `continue` command. Unfortunately, code contained in a `extra_code`
  parameter is not visible in source form.
- alternatively, one can just raise an exception and it will be caught by the
  top level exception handler in run_recipe.py and then drop into an ipdb
  debugger session if the `--debug` flag has been set.
  It is also necessary to set `--single-threaded`, otherwise the data and other
  worker/thread-local variables are not accessible.
