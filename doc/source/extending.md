Extending
=========

## Adding a new metric/variable
Add a new data class to `variables/variables.py`:
- the class needs to be decorated with the `register_variable` class decorator to register it in the central `VariableRegistry`
- the class needs to have at least an `alias` to address it
- if the class is simply calculating statistics over a signal, set the `signal_name` attribute to the signal name
- if the class is the result of doing more complicated calculations over the input data:
  - add a new operation in `operations.py`
