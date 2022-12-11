# ---

from yaml import YAMLObject


class Recipe(YAMLObject):
    yaml_tag = u'!Recipe'
    pass

class Evaluation(YAMLObject):
    yaml_tag = u'!Evaluation'
    pass

class Plot(YAMLObject):
    yaml_tag = u'!Plot'
    pass

class Task(YAMLObject):
    yaml_tag = u'!Task'
    def __init__(self):
        pass

