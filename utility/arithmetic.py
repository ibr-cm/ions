import ast
import operator

def evaluate_simple_arithmetic_expression(expression_string:str):
    r"""
    Evaluate a simple arithmetic expression of the form 'x{+,-}y'

    Parameters
    ----------
    expression_string : str
        the expression to be evaluated

    Returns
    -------
    the result of the evaluated expression string
    """
    allowed_operators = { ast.Add: operator.add, ast.Sub: operator.sub }
    expression = ast.parse(expression_string, mode='eval')
    op = expression.body
    if isinstance(op, ast.BinOp) \
        and type(op.op) in allowed_operators \
        and isinstance(op.left, ast.Constant) \
        and isinstance(op.right, ast.Constant):
            result = allowed_operators[type(op.op)](op.left.value, op.right.value)
            return result
    elif isinstance(op, ast.Constant):
        return op.value
    else:
        raise TypeError(f'\'{expression_string}\' is not a valid expression in this context')
