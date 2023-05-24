Using transforms
================

When dealing with metrics that need to be calculated from one or by combining
multiple signals, the use of a transform becomes necessary.

A transform should output a list of `pandas.DataFrame`.

When developing code for a transform or debugging errors, it can be really
useful to start an interactive console by adding:
```
start_ipython_dbg_cmdline(user_ns=locals())
```
into the code in the recipe and running single-threaded by adding `--worker
1 --single-threaded` to the command line. This allows inspection and
manipulation of the loaded data in a comfortable REPL.

There are currently two types of transform implemented:
- `FunctionTransform`
- `GroupedFunctionTransform`

#### `FunctionTransform`
This is for applying a function to every value in a selected column of the data
and saving the result in another (or the same) column.
The user defined unary function defined by the `function` parameter is executed
for every value, for every `pandas.DataFrame` in the selected dataset

#### `GroupedFunctionTransform`
This is for dividing the input `pandas.DataFrame`s into partitions based on 
sharing the same values in the columns given by `grouping_columns`. On each of
these partitions a user defined function is applied. The unary function takes
a `DataFrame` as argument and returns either a `DataFrame`, a scalar value or an
arbitrary object as result. In the case of a scalar, the parameter `aggregate`
should be set; the first row of the partition `DataFrame` is taken and the
single value output of the function is added in a new column. In the case of an
arbitrary object, the parameter `raw` should be set; the output of the function
is then passed on without modification, most likely for export as a JSON
representation of the object.
