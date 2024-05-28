Quick start for analysing and plotting a signal
===============================================

If one just wants to create a simple line/boxplot, this can be easily achieved 
by making a copy of `examples/lineplot.yaml` and changing a few paths,
the signal name and the name associated with the dataset holding the signal
data. Additionally, you may have to add a tag for each parameter of your study.

In `evaluation`:
- in `tags`:
  - change `sensors` to the name of the parameter of your simulation study and
    change the associated `regex` and `transform` to extract the value of the
    parameter. If your parameter is not listed in `iterationvars` but the
    `runAttr` or `runParam` table, change the `iterationvars` to
    `attributes`/`parameters`.

- in `extractors`:
  - change the name of the extraction task from `cbr` to the name you want to
    assign to the extracted signal data
  - change `input_files` to point to the data files
  - change `signal` to the `vectorName` of your signal
  - change `alias` from `cbr` to the name of the column the extracted data
    should be placed in
  - change the `sensors` in `additional_tags` to the name of your parameter

- in `exporter`:
  - change `dataset_name` to the name you assigned in `extractors`
  - change `output_filename` to point to the path the data should be saved to

In `plot`:
- in `reader`:
  - change the name of the extraction task from `cbr` to the name you want to
    assign to the extracted signal data
  - change `input_files` to point to the data files

- in `tasks`:
  - change the name of the plotting task from `cbr` to a unique name
  - change `dataset_name` to the name you assigned in `reader`
  - change `x` to the name of the study parameter to plot on the x-axis, if the
    rate of vehicle equipment is not applicable
  - change `y` to the name of the `alias` assigned in `extractors`
  - change `hue` to the name of the tag defined in `tags`
    - if there are additional study parameters, place one each in `row` and
      `column`. This will generate a plot with multiple columns/rows, one for
      each value of the assigned column/row variable.
  - change `output_file` to point to the path of the file the plot should be saved to
  - if one wants a boxplot instead of a lineplot, change the `plot_type` to
    `box`

Now the recipe can be processed by `run_recipe.py` and should produce
a lineplot.
Using pipenv as package manager:
```
pipenv python run run_recipe.py <path to the recipe>
```
