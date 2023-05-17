Running on a cluster
====================

By default, the framework launches a local dask cluster with four workers, which
is good enough for testing and preparing a recipe on a subset of data. But for
evaluating large datasets, one will want to make better use of the resources
available.

For utilizing a SLURM cluster, add the following parameters to the
`run_recipe.py` call:
```
--slurm          \  # use SLURM
--nodelist i4,i5 \  # the SLURM nodelist, see SLURM documentation
--worker 16         # use 16 worker
```

The amount of workers should be set to the number of cores available on one
node. Be aware that pandas uses
[numexpr](https://github.com/pydata/numexpr#what-is-numexpr) to accelerate some
calculations and may spawn multiple threads in each worker. For better
performance, scalability and resource utilization, one should set
the environment variable `NUMEXPR_MAX_THREADS` appropriately, either to `1` or
`2`, after testing and observing with a small subset of the data. Default is the
number of cores or 8, whichever is less.

If you are using a SLURM node on a partition other than the default (check with
`sinfo -Nl`), add `--partition <partition_name>` to the call.

A already running dask cluster can be used by appending `--cluster
<cluster_address>` to the command line. See the setup
[here](https://distributed.dask.org/en/stable/quickstart.html#setup-dask-distributed-the-hard-way)

For debugging purposes a single-threaded mode is available, selectable by adding
`--single-threaded` to the command line.

