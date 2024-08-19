import yaml

from typing import Callable

def load_yaml_from_file(file):
    r"""
    Read and evaluate the YAML contained in the file with the given name
    """
    f = open(file, mode = 'rt', encoding = 'utf_8')
    x = yaml.unsafe_load(f.read())
    f.close()
    return x

def construct_bool(loader, node):
    x = loader.construct_scalar(node)
    if x == 'false' or x == 'False':
        x = False
    elif x == 'true' or x == 'True':
        x = True
    else:
        raise ValueError(f'wrong value for boolean type: {x}')
    return x

def construct_numeral(loader, node, type_constructor:Callable = int):
    r"""
    Construct a numeral scalar and cast to the desired type
    """
    x = loader.construct_scalar(node)
    x = type_constructor(x)
    return x

def decode_node(loader, node):
    match type(node):
        case yaml.ScalarNode:
            match node.tag:
                case '!include':
                    x = load_yaml_from_file(node.value)
                case 'tag:yaml.org,2002:bool' | '!bool' | '!!bool':
                    x = construct_bool(loader, node)
                case 'tag:yaml.org,2002:float' | '!float' | '!!float':
                    x = construct_numeral(loader, node, float)
                case 'tag:yaml.org,2002:int' | '!int' | '!!int':
                    x = construct_numeral(loader, node, int)
                case 'tag:yaml.org,2002:complex' | '!complex' | '!!complex':
                    x = construct_numeral(loader, node, complex)
                case 'tag:yaml.org,2002:null' | '!null' | '!!null':
                    x = None
                case 'tag:yaml.org,2002:tuple' | '!tuple' | '!!tuple':
                    x = eval(node.value) # pylint: disable=W0123:eval-used
                case 'tag:yaml.org,2002:code' | '!code' | '!!code':
                    x = eval(node.value) # pylint: disable=W0123:eval-used
                case 'tag:yaml.org,2002:eval' | '!eval' | '!!eval':
                    x = eval(node.value) # pylint: disable=W0123:eval-used
                case 'tag:yaml.org,2002:dict' | '!dict' | '!!dict':
                    x = eval(node.value) # pylint: disable=W0123:eval-used
                case _:
                    # use the default scalar constructor
                    x = loader.construct_scalar(node)
        case yaml.MappingNode:
            x = loader.construct_mapping(node)
        case yaml.SequenceNode:
            x = loader.construct_sequence(node)
        case _:
            x = None
    return x


def proto_constructor(class_constructor, set_defaults = None):
    def constructor(loader, node):
        parameters = {}
        # construct proper python objects from the YAML definitions to pass as class parameters
        for pair in node.value:
            # decode member variable name
            x = decode_node(loader, pair[0])
            # decode member variable value
            y = decode_node(loader, pair[1])
            parameters[x] = y

        return class_constructor(**parameters)

    return constructor


def include_constructor(loader, node):
    x = load_yaml_from_file(node.value)
    return x


def register_constructors():
    r"""
    Register YAML constructors for all the custom tags
    """
    yaml.add_constructor(u'!include', include_constructor)

register_constructors()
