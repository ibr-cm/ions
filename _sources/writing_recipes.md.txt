Writing Recipes
===============

A few things are very important:

- Be very observant about indentation.
  YAML uses indentation for scoping attributes to objects/collections, i.e. structural scope, just like Python.
  This can lead to a `yaml.parser.ParserError`, a `TypeError` about missing
  arguments to a `__init__` constructor  or just silent misattribution of a
  parameter to the wrong object. Be attentive of indentation and check the
  recipe with the help of the `--dump-recipe-only` flag
  See the chapter of the YAML specification on [Indentation Spaces](https://yaml.org/spec/1.2.2/#61-indentation-spaces) for details.

The key points to remember are:

- don't use tabs for indentation (see link above)

- the indentation depth is not important, but it has to be consistent throughout the whole recipe document

- use `--dump-recipe-only` to print the parsed recipe to stdout end exit
  without executing it. This allows for checking parameter values and escaping
  as well as indentation error that have not been caught by the parser.

- file paths are always parsed as regular expressions, so one should add a `$`
  add the end of an expression for matching input files such as `.vec` to
  exclude journal files (`.vec-journal`) which would produce a lot of
  unnecessary errors

- A path given in a recipe is evaluated relative to the current working directory of execution.
  It is thus advisable to use absolute paths whenever possible.
  Otherwise, if one uses e.g. `pdm run` with the `-p` flag to set an
  alternative virtualenv root than the current directory, all relative paths in
  the recipe will be interpreted as relative to the current directory rather than the virtualenv root.

- A parameter of a task must be defined only once. Do not redefine a parameter
  for convenience. This might work in practice, but that is an implementation
  detail that one must not rely upon (the YAML specification does not require
  a order on mappings, see [mapping key
  order](https://yaml.org/spec/1.2.2/#3221-mapping-key-order)). Put
  differently, the parameters of a task are block mappings that are then used
  as keys into a Python dictionary.

