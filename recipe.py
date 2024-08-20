# ---

from yaml import YAMLObject


class Recipe(YAMLObject):
    yaml_tag = '!Recipe'
    pass

class Evaluation(YAMLObject):
    yaml_tag = '!Evaluation'
    pass

class Plot(YAMLObject):
    yaml_tag = '!Plot'
    pass

class Task(YAMLObject):
    yaml_tag = '!Task'
    def __init__(self):
        pass

