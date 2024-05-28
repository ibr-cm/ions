Extending
=========

## Adding a new metric/variable
Note: The following applies to both input as well as output variables. I.e., if you add new simulation parameters, these need to be added here as well as new evaluation metrics.

Add a new data class to `variables/variables.py`:
- the class needs to be decorated with the `register_variable` class decorator to register it in the central `VariableRegistry`
- the class needs to have at least an `alias` to address it
- if the class is simply calculating statistics over a signal, set the `signal_name` attribute to the signal name
- if the class is the result of doing more complicated calculations over the input data:
  - add a new operation in `operations.py`



## Adapt evaluation/operations.py
Add new variable to regex_map in function add_parameters(self, data) of class ExtractRunParametersTagsOperation(DbOperation)

## Adapt evaluation/evaluation_runner.py
Add new variable to tag_set in function generate_output_file_name of class EvaluationRunner

## Adapt plotting/utility.py
Add new variable to mapping in function map_label_column_to_str
