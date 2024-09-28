from typing import Callable, Iterable, Union

import yaml


def load_yaml_from_file(file_name:str):
    r"""
    Read and evaluate the YAML contained in the file with the given name, construct the corresponding python object
    and return it.

    Parameters
    ----------
    file_name: str
        The path to the file to load.

    Returns
    -------
    Any:
        The object parsed from the YAML definitions in the given file.
    """
    with open(file_name, mode = 'rt', encoding = 'utf_8') as text_file:
        parsed_object = yaml.unsafe_load(text_file.read())
    return parsed_object

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

def construct_joined_sequence(loader:yaml.Loader, sequence:Union[yaml.nodes.SequenceNode, Iterable]):
    if isinstance(sequence, yaml.nodes.SequenceNode):
        # Construct the nodes of the SequenceNode into an Iterable.
        joined_sequence = decode_node(loader, sequence)
    elif isinstance(sequence, Iterable):
        # First construct all the contained nodes.
        nodes_to_join = []
        for node in sequence:
            x = decode_node(loader, node)
            nodes_to_join.append(x)

        # Now join all the nodes of the sequence depending on the type.
        if all(map(lambda x: isinstance(x, str), nodes_to_join)):
            # A list of strings is joined into a single string.
            joined_sequence = ''.join(nodes_to_join)
        elif all(map(lambda x: isinstance(x, list), nodes_to_join)):
            # A list of lists is joined into a single list.
            joined_sequence = []
            for elm in nodes_to_join:
                joined_sequence.extend(elm)
        elif all(map(lambda x: isinstance(x, dict), nodes_to_join)):
            # A list of dictionaries is joined into a single dictionary.
            joined_sequence = {}
            for elm in nodes_to_join:
                joined_sequence.update(elm)
        else:
            # Inhomogenous sequences are not supported.
            raise TypeError(f"Unsuitable types for joining: {[ type(x).__name__ for x in nodes_to_join ]}"
                            f"\n{nodes_to_join= }")
    else:
        # Cover all remaining cases.
        raise TypeError(f"Unsuitable sequence for joining:  {type(sequence).__name__}" f"\n{sequence}")

    return joined_sequence

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
            match node.tag:
                case '!join':
                    # Join all the elements of the sequence.
                    x = construct_joined_sequence(loader, node.value)
                case _:
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
    yaml.add_constructor('!include', include_constructor)
    yaml.add_constructor('!join', construct_joined_sequence)
