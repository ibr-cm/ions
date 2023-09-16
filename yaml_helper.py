import yaml

def load_yaml_from_file(file):
    f = open(file, mode='r')
    x = yaml.unsafe_load(f.read())
    f.close()
    return x

def construct_bool(loader, node):
    x = loader.construct_scalar(node)
    if x == 'false' or x == 'False':
        x = False
    else:
        x = True
    return x

def construct_float(loader, node):
    x = loader.construct_scalar(node)
    x = float(x)
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
                    x = construct_float(loader, node)
                case 'tag:yaml.org,2002:null' | '!null' | '!!null':
                    x = None
                case 'tag:yaml.org,2002:tuple' | '!tuple' | '!!tuple':
                    x = eval(node.value)
                case 'tag:yaml.org,2002:code' | '!code' | '!!code':
                    x = eval(node.value)
                case _:
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
        for pair in node.value:
            # decode member variable name
            x = decode_node(loader, pair[0])
            # decode member variable value
            y = decode_node(loader, pair[1])
            parameters[x] = y
            # logi(f'------>>>>>      {x=}  {y=}')

        # logi(f'------>>>>>      {d=}')
        if set_defaults:
            parameters = set_defaults(parameters)
        return class_constructor(**parameters)

    return constructor

# def set_defaults_from_dict(self, d):
#     for k in d:
#         if not hasattr(self, k):
#             setattr(self, k, d[k])

# def set_defaults(self):
#     d = {
#            'alpha': 0.9
#          , 'xlabel': lambda self: self.x
#          , 'ylabel': lambda self: self.y
#          , 'bin_size': 10
#          , 'title_template': None
#          , 'legend_location': 'best'
#          , 'yrange': None
#          , 'colormap': None
#         }

#     self.set_defaults_from_dict(d)


def include_constructor(loader, node):
    x = load_yaml_from_file(node.value)
    return x


def register_constructors():
    yaml.add_constructor(u'!include', include_constructor)

register_constructors()
