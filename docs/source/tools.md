# Tools

## run_recipe.py
This is the main utility, described in [Evaluation & Plotting](evaluation_plotting.md)

## zjqc.sh
This can be used for inspecting the gzip compressed output JSON from `eval.py`.
Dependencies:
- [jq](https://stedolan.github.io/jq/) (_A lightweight and flexible command-line JSON processor_)
- zcat (part of [gzip](https://www.gnu.org/software/gzip/)) (see gzip(1))
- (**optional**) [bat](https://github.com/sharkdp/bat) (_A cat(1) clone with syntax highlighting and Git integration._)
usage:
`./zjqc.sh <JSON FILE>`
