Best practices
==============

This is a collection of best practices to consider when using this framework:

- test the extraction and plotting with a subset of your data first. No need to
  waste time & energy if there's a typo somewhere or the parameters of a task are
  not appropriately set.
- if you just want to extract data, add `--eval-only` to your command line. Similarly, if
  you just want to plot, add `--plot-only`
- test the regular expression used for the paths to the input data. One might
  have hurried and copied an expression with globbing as used by a shell.
- test the regular expression used for tag extraction in a python REPL on the
  actual string values in the database
- only three parameters for partitioning the data are supported in plotting
  tasks. If more are needed, use a `GroupedFunctionTransform` to add another
  column to the DataFrame that combines multiple parameters into a string that
  then can be used in the `hue`, `row` or `column` parameter of a plotting task.
- running `run_recipe.py` with the parameter `--plot-task-graphs` generates
  plots with the task graph in the directory for temporary files (set with
  `--tmpdir`.
- when developing code for a transform or debugging errors, it can be really
  useful to start an interactive console by adding:
  ```
  start_ipython_dbg_cmdline(user_ns=locals())
  ```
  into the code in the recipe and running single-threaded by adding `--worker
  1 --single-threaded` to the command line

