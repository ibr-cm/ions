from typing import Any, Callable, Optional, Union

def compile_and_evaluate_function_definition(source:str, function_name:str , global_environment:dict[str, Any]) -> tuple[Callable, dict[str, Any]]:
    r"""
    Evaluate the sequence of code statements in the given `source` and return the function object bound to the specified `function_name` name.

    Parameters
    ----------
    source : str
        The source code of the code snippet to compile and evaluate.
    function_name : str
        The name of the function to return.
    global_env : dict
        The global runtime environment to evaluate the given code in.
    
    Returns
    -------
    Callable
        The resulting function object.
    dict
        The runtime environment of the function.
    
    Raises
    ------
    SyntaxError
        If the supplied source code has a syntax error.
    ValueError
        If the supplied source code contains null bytes.
    """

    # Compile the sequence of code statements in the given code fragment.
    compiled_function_code = compile(source, filename='<string>', mode='exec')
    # Actually evaluate the code within the given namespace to allow
    # access to all the defined symbols, such as helper functions that are not defined inline.
    eval(compiled_function_code, global_environment) # pylint: disable=W0123:eval-used
    
    # Get the function object of the desired function.
    function = eval(function_name, global_environment) # pylint: disable=W0123:eval-used

    return function, global_environment


class ExtraCodeFunctionMixin:
    r"""
    A mixin class for providing the functionality to compile and evaluate a
    function and an additional, optional code fragment within a separate global environment.
    """
    def evaluate_function(self, function:Union[Callable, str], extra_code:Optional[str]) -> tuple[Callable, dict[str, Any]]:
        r"""
        Compile and evaluate the given function and an additional, optional
        code fragment within a separate global environment and return the
        executable function object.

        Parameters
        ----------
        function : Union[Callable, str]
            The name of the function or a function object.

        extra_code : Optional[str]
            This can contain additional code for the function definition, such as the definition of a function with
            multiple statements or split into multiple functions for readibility, i.e. every use case where a single
            simple python statement doesn't suffice.
        
        Returns
        -------
        Callable
            The resulting function object.
        dict[str, Any]
            The runtime environment of the function.
        """
        # Create a copy of the global environment for evaluating the extra code fragment so as to not pollute the
        # global namespace itself.
        global_env = globals().copy()

        if isinstance(function, str) and isinstance(extra_code, str):
            function_object, global_env = compile_and_evaluate_function_definition(extra_code, function, global_env)
            return function_object, global_env
        elif not isinstance(function, str) and isinstance(function, Callable):
            evaluated_function = function
            return evaluated_function, global_env
        elif isinstance(function, str):
            evaluated_function = eval(function, global_env) # pylint: disable=W0123:eval-used
            return evaluated_function, global_env
        else:
            raise NotImplementedError(f'Cannot compile: {function=}  {extra_code=}')