Best practices
==============

This is a collection of best practices to consider when using this framework:

- keep the input data (SQLite3/feather files) on a SSD disk, otherwise the
  random access pattern on the files over NFS will lead to rather severe
  performance losses
- test the extraction and plotting with a subset of your data first. No need to
  waste time & energy if there's a typo somewhere or the parameters of a task are
  not appropriately set.
- increase the verbosity with `-vv...` to see what extractors, transformers and
  exporters do, e.g. which columns are extracted, are in- and output of transformers
   and get categorized.
- if you are working with a subset of your data, be sure to test the assumptions
  made in your code on the whole dataset, e.g. handling of NaN values. Testing
  for those edge cases is a good way to verify your simulation code too.
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
- when using `pandas.DataFrame.groupby` to partition up the data, e.g. using
  the `GroupedFunctionTransform`, try limiting the number of keys used for
  partitioning and the size of the input `DataFrame`s to minimise processing
  time and memory usage. Only concatenate the input into a large `DataFrame` if
  the operation has to happen over all data or over subsets of the data that
  can't otherwise be easily selected.
- when extracting a signal with its associated position data, a lot of SQL JOIN
  operations over the `eventNumber` are being executed. This is a fairly slow
  procedure since there's no index over the `eventNumber` column (inspect the
  `sqlite_master` table in the result database to verify this). To improve
  performance, one can construct an index over `eventNumber`:
  ```sh
  #!/bin/sh

  for file in $@
  do
      \time sqlite3 $file 'CREATE INDEX eventNumber_index ON vectorData(eventNumber);' \
              && echo "created index over eventNumber in" $file
              done

  ```
  This increases storage usage by a factor of two, so it's only to be used on a
  copy of the dataset and the resulting files should be deleted after use.
  Note: this only applies to extraction from the databases, all stages after
  that are not influenced.
