# Tools

## eval.py & plot.py
These are the main utilities, described in [Evaluation & Plotting](evaluation_plotting.md)

## GenSlurmBatch.py
This can be used to generate a batch file for running simulations using SLURM.

## GenSlurmBatchEval.py
This can be used to generate a batch file for running evaluations using SLURM.

## launch_slurm_batch.sh
This can be used to launch a SLURM batch and have a command executed depending on the outcome of the batch job.

## zjqc.sh
This can be used for inspecting the gzip compressed output JSON from `eval.py`.
Dependencies:
- [jq](https://stedolan.github.io/jq/) (_A lightweight and flexible command-line JSON processor_)
- zcat (part of [gzip](https://www.gnu.org/software/gzip/)) (see gzip(1))
- (**optional**) [bat](https://github.com/sharkdp/bat) (_A cat(1) clone with syntax highlighting and Git integration._)
usage:
`./zjqc.sh <JSON FILE>`

## diff_databases.py
A tool for checking for differences between recorded signal data in a pair of SQLite databases.  
This is also a simple example for using parts of the evaluation framework for specific task
(though most of the heavy lifting here is done by `pandas`).  
usage:
`pipenv run python diff_databases.py -e <vec file 0> <vec file 1>`

`-e` checks the timestamps of the emitted signals for each vehicle and print differences  
	 (see script for the list of checked signals)

## ConvertJSON.py
This can be used to perform conversions on JSON files.  
Right now it just supports converting older JSON files with `draft` generation
rule label to `dynamic`  
usage:  
`./ConvertJSON.py -c draft-to-dynamic -d <OUTPUT DIR> <FILES...>`

## update_docs_sphinx.sh
Build and publish this documentation.
