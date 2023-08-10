import yaml

def decode_node(loader, node):
    match type(node):
        case yaml.ScalarNode:
            if node.tag == '!include':
                f = open(node.value, mode='r')
                x = yaml.unsafe_load(f.read())
                f.close()
            else:
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
        # m = loader.construct_mapping(node)
        parameters = {}
        for pair in node.value:
            # logi(f'------>>>>>      {pair[0]=}  {pair[1]=}')
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

