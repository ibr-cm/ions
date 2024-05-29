# Evaluation & Plotting

## Key Technologies

The framework is built on top of [pandas](https://pandas.pydata.org/),
[seaborn](https://seaborn.pydata.org/index.html) and
[dask](https://docs.dask.org/en/stable/)

Dask is used as a way to build task dependency graphs using
[`dask.delayed`](https://docs.dask.org/en/latest/delayed.html) and for
scalable parallelisation of the execution of those tasks, either locally on
a desktop/laptop or remotely on a (SLURM) cluster using
[Dask.distributed](https://distributed.dask.org/en/stable/) and
[Dask-Jobqueue](https://jobqueue.dask.org/en/latest/index.html).

There are a few key concepts necessary for using the framework effectively:
- [`pandas.Series`](https://pandas.pydata.org/pandas-docs/stable/user_guide/dsintro.html#series)
- [`pandas.DataFrame`](https://pandas.pydata.org/pandas-docs/stable/user_guide/dsintro.html#dataframe)

The main data structure of the framework is the
[`pandas.DataFrame`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), which is
similar to a spreadsheet or a table in a SQL database. For everything
interesting you will need some knowledge how to index, partition and apply
a function to a `DataFrame`.
A short [introduction](https://pandas.pydata.org/pandas-docs/stable/user_guide/dsintro.html)
to the data structures used in pandas.
Reading the documentation for
[`pandas.DataFrame.groupby`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.groupby.html#pandas.DataFrame.groupby)
is advisable.


## Getting Insight

The evaluation and plotting is done by running `run_recipe.py` with the name of
the 'recipe' YAML file, which contains the steps for processing the results of a batch of simulation
runs. For a minimal introduction to YAML and some specifics of PyYAML see the [PyYAML documentation](https://pyyaml.org/wiki/PyYAMLDocumentation)

### Recipes

The recipe describes the individual tasks as a list of key-value pairs; internally the data and
operations are constructed (by [dask](https://docs.dask.org/en/latest/)) into an
dependency/task graph.

A recipe can contain two phases: `evaluation` and `plot`. Each phase is optional.
The `evaluation` phase itself consists of three sub-phases:
- `extractors`: for extracting the desired data from the input databases
- `transforms`: for processing the extracted data in some way
- `exporter`: for saving the extracted and possibly processed data

The `plot` phase consists of three sub-phases:
- `reader`: for loading the data exported from the `evaluation` sub-phase
- `transforms`: for processing the loaded data in some way
- `tasks`: for actually plotting the loaded and possibly processed data

The sub-phases are evaluated in this order. Each sub-phase consists of a list of tasks to
execute and each task usually has a dependency on a task in the previous
sub-phase. If a task is not depended upon by another task, it will not be part
of the dependency/task graph and will thus not be executed.

Each task either creates or modifies a named 'dataset', a list of
`pandas.DataFrame`s. Usually an extractor creates a `pandas.DataFrame` for
each input file and stores the resulting list under the user defined
`dataset_name` in an internal dictionary, all `transforms` are then executed
over each `DataFrame` in that list separately and then written to disk, either
separately or concatenated into a single `DataFrame` and then written to disk.

The basic structure of a recipe is thus, for the evaluation phase:
```
evaluation:
    extractors:
        dataset0: !extractor_class
            parameter0: "value"

    transforms:
        transformed_dataset0: !transform_class
            dataset_name: "dataset0"
            parameter0: "value"

    exporter:
        name0: !exporter_class
            dataset_name: "transformed_dataset0"
            parameter0: "value"
```
For the plotting phase:
```
plot:
    reader:
        dataset0: !reader_class
            input_files:
                - "/path/regular/expression0"
                - "/path/regular/expression1"

    transforms:
        transformed_dataset0: !transform_class
            dataset_name: "dataset0"
            output_dataset_name: "dataset0"
            parameter0: "value"

    tasks:
        plot_task0: !plotting_class
            dataset_name: "transformed_dataset0"
```
For simplicity only one task is listed for each sub-phase, but an arbitrary
number of tasks is possible.

For ease of use, the following omissions are possible:
- the `transforms` phase is optional, chaining transforms is possible
- the `exporter` and `reader` phases are optional, the `plot` phase can just use
  the datasets extracted in the `evaluation` phase

The first line of the definition of a task has the format '<task_name>: !<task_classname>'
and defines the name and the type of the transform. The type is just the
class name (or more precisely, the YAML tag assigned to the class, but they are
literally the same) of the desired operation. What follows are the parameters
of the constructor for the class.

In the `extractors` and `reader` sub-phase, the collection of data that is to be
extracted, processed and plotted is given a name so as to allow referencing it
in the task of other sub-phases. In the example above, the name given is
`dataset0`.
Each task has at least one parameter `dataset_name`, which references the input
dataset the task operates upon. A transform will always have parameter
`output_dataset_name` to assign a name to the result of the operation. This
assignment can overwrite previously defined names, and thus also free the
associated data if they are not being depended upon by another instance.

Most of the documentation of the parameters of the actual components is in the
API documentation, e.g. the documentation one needs for just plotting is in
`plots.PlottingTask`, specifically in the documentation of the parameters of the
constructor for that class.

#### Tags
The `evaluation` phase also supports assigning tags to the extracted data. A tag
is a property shared among a subset of the input data, e.g. the repetition
number of a run, the run number or the rate at which vehicles are being equipped
with V2X hardware.

The syntax for the tag definition is as follows:
```
evaluation:
    tags:
        attributes:
            repetition: |
                [{
                    'regex': r'repetition'
                  , 'transform': lambda v: int(v)
                }]
        iterationsvars:
            tag_name_1: |
                [{
                    'regex': r'anotherExampleRE.*'
                  , 'transform': lambda v: str(v)
                }]
        parameters:
            tag_name_2: |
                [{
                    'regex': r'exampleRE.*'
                  , 'transform': lambda v: str(v)
                },
                {
                    'regex': r'exampleRE2.*'
                  , 'transform': lambda v: str(v)
                },
                ]
```
The `attributes`, `iterationsvars` and `parameters` are predefined categories
for the tags and are extracted from different places in the input database.
The general procedure involves using the regular expression (python flavour, [syntax](https://docs.python.org/3/library/re.html#regular-expression-syntax))
defined by the `regex` key to match on the name of the attribute and
then applying the unary function defined by the `transform` key to the value in
the column associated with the category.

The tags are extracted from:
- `attributes`: the `runAttr` table
    - the `regex` matches on the value in the `attrName` column
    - the `transform` is applied to the value in the `attrValue` column
- `iterationvars`: the row with `attrName=='iterationvars'` in the `runAttr` table
    - the `regex` matches on the value of the `attrValue` column of the row
    - the `transform` is applied to the value matched by the regular expression
- `parameters`: the `runParam` table
    - the `regex` matches on the value in the `paramKey` column
    - the `transform` is applied to the value in the `paramValue` column

Multiple regular expressions can be bound to the same tag, in case of a heterogeneous data set or typing errors.

The built-in tag definitions can be found in `tag_regular_expressions.py`.

### Examples

Example recipes can be found in the `examples` directory in the root of this
repository:
- `lineplot.yaml`: a basic recipe for producing a CBR-over-MPR lineplot. One
  should probably start with this as template.
- `CUI.yaml`: this calculates, for every vehicle, the mean of the differences between consecutive receptions of
  a CAM and plots them as a lineplot. This is probably the second template to
  look at, as it uses `GroupedFunctionTransform` to partition the input data
  by MPR and the name of the module emitting the signal used as marker for CAM
  emission.
- `recipe.yaml`: a more elaborate recipe showcasing all possible options
- `statistic.yaml`: this extracts the results for the statName
  `LemObjectUpdateInterval:stats` from the `statistic` table and saves them,
  then plots the mean of the values (from the `statMean` column of the
  table) of those over the market rate.
- `boxstats.yaml`: showcases using a custom function to calculate the values
  needed for a boxplot, forward them as a python list to the exporter and save
  them as a JSON file
- `sqlextractor.yaml`: a showcase for the generic SQLite extractor

