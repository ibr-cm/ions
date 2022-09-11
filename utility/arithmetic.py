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
        and isinstance(op.left, ast.Num) \
        and isinstance(op.right, ast.Num):
            result = allowed_operators[type(op.op)](op.left.n, op.right.n)
            return result
    elif isinstance(op, ast.Num):
        return op.n
    else:
        raise TypeError(f'\'{expression_string}\' is not a valid expression in this context')
