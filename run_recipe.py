#!/usr/bin/python3

import pprint
import sys
import argparse

# ---

import yaml

from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

import pandas as pd

import dask
import dask.distributed
from dask.distributed import LocalCluster
from dask_jobqueue import SLURMCluster

from dask.distributed import Client

# ---

from recipe import Recipe, RawExtractor

from sql_queries import generate_signal_query

# ---

def execute_evaluation_phase(recipe:Recipe, options):
    print(f'execute_evaluation_phase: {recipe}  {recipe.name}')

    op_registry = {}
    op_registry['raw'] = RawExtractor


    if not hasattr(recipe.evaluation, 'extractors'):
        print('execute_evaluation_phase: no `extractors` in recipe.Evaluation')
        return

    evaluation = recipe.evaluation

    data_repo = {}
    for extractor_name in recipe.evaluation.extractors:
        extractor = recipe.evaluation.extractors[extractor_name]

        if extractor_name in options.extraction_overrides:
            extractor.input_files = [ options.extraction_overrides[extractor_name] ]
            print(f'overriding {extractor_name} with {extractor.input_files}')

        delayed_data = extractor.prepare()
        # print(f'{extractor=}')
        # print(f'{delayed_data.memory_usage(deep=True) = }')
        # print(f'-<-<-<-<-<-<-')
        data_repo[extractor_name] = delayed_data

    if not hasattr(recipe.evaluation, 'transforms'):
        print('execute_evaluation_phase: no `transforms` in recipe.Evaluation')
        return data_repo

    for transform_name in recipe.evaluation.transforms:
        transform = recipe.evaluation.transforms[transform_name]
        transform.set_data_repo(data_repo)
        transform.execute()

    jobs = []
    for exporter_name in recipe.evaluation.exporter:
        exporter = recipe.evaluation.exporter[exporter_name]

        if exporter_name in options.export_overrides:
            exporter.output_filename = options.export_overrides[exporter_name]
            print(f'overriding {exporter_name} with {exporter.output_filename}')

        exporter.set_data_repo(data_repo)
        job = exporter.execute()
        jobs.append(job)


    print(f'{jobs=}')

    if options.plot_task_graphs:
        for i in range(0, len(jobs)):
            jobs[i].visualize(f'{options.tmpdir}/dask_task_graph_evaluation_job-{i}.png')

    # now actually compute the constructed computation graph
    dask.compute(*jobs)

    print('=-!!'*40)


def execute_plotting_phase(recipe:Recipe, options):
    print(f'execute_plotting_phase: {recipe}  {recipe.name}')

    data_repo = {}

    for dataset_name in recipe.plot.reader:
        reader = recipe.plot.reader[dataset_name]
        print(f'plot: loading dataset: "{dataset_name=}"')
        if dataset_name in options.reader_overrides:
            reader.input_files = options.reader_overrides[dataset_name]
            print(f'plot: execute_plotting_phase overriding input files for "{dataset_name}": "{reader.input_files=}"')
        data = reader.read_data()
        data_repo[dataset_name] = data

    print('<<<-<-<--<-<-<--<-<-<')
    print(f'plot: {data_repo=}')
    print('<<<-<-<--<-<-<--<-<-<')

    for task_name in recipe.plot.transforms:
        task = recipe.plot.transforms[task_name]
        task.set_data_repo(data_repo)
        task.execute()

    jobs = []
    for task_name in recipe.plot.tasks:
        task = recipe.plot.tasks[task_name]
        print(f'plot: {task_name=}')
        print(f'plot: {task=}')
        print(f'plot: {task.dataset_name=}')
        # print(f'plot: loading data...')
        # task.load_data()

        if task_name in options.plot_overrides:
            task.output_file = options.plot_overrides[task_name]
            print(f'overriding {task_name} with {task.output_file}')

        task.set_data_repo(data_repo)
        print(f'plot: executing plotting tasks...')
        job = task.execute()
        # print(f'plot: {job=}')
        jobs.append(job)

    if options.plot_task_graphs:
        for i in range(0, len(jobs)):
            jobs[i].visualize(f'{options.tmpdir}/dask_task_graph_plotting_job-{i}.png')

    print(f'plot: {jobs=}')
    r = dask.compute(*jobs)
    print(f'plot: {r=}')


def process_recipe(options):
    f = open(options.recipe, mode='r')

    recipe = yaml.unsafe_load(f.read())

    pprint.pp(recipe)
    pprint.pp(recipe.__dict__)


    output = dump(recipe, Dumper=Dumper)

    if not hasattr(recipe, 'evaluation'):
        print('process_recipe: no Evaluation in recipe')
        return

    if not options.plot_only:
        execute_evaluation_phase(recipe, options)

    if options.eval_only:
        return

    execute_plotting_phase(recipe, options)


def extract_dict_from_string(string):
    d = dict()
    for token in string.split(','):
        key, value = token.strip(',').split(':')
        d[key] = value
    return d


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('recipe', help='input recipe')

    parser.add_argument('--override-extractor', type=str, help='override extractor parameters')
    parser.add_argument('--override-exporter', type=str, help='override exporter parameters')

    parser.add_argument('--override-reader', type=str, help='override reader parameters')
    parser.add_argument('--override-plot', type=str, help='override plot parameters')

    parser.add_argument('--eval-only', action='store_true', default=False, help='run eval phase only')
    parser.add_argument('--plot-only', action='store_true', default=False, help='run plot phase only')

    parser.add_argument('--worker', type=int, default=4, help='the number of worker processes')

    parser.add_argument('--cluster', type=str, help='cluster address')

    parser.add_argument('--slurm', action='store_true', default=False, help='use SLURM cluster')
    parser.add_argument('--nodelist', type=str, help='nodelist for SLURM')

    parser.add_argument('--tmpdir', type=str, default='/opt/tmpssd/tmp', help='directory for temporary files')

    parser.add_argument('--plot-task-graphs', action='store_true', default=False, help='plot the evaluation and plotting phase task graph')

    args = parser.parse_args()

    if args.slurm:
        if not args.nodelist:
            raise Exception('A nodelist ist required when using SLURM')

    def set_dict_arg_from_string(option, arg_name):
        if option:
            option_dict = extract_dict_from_string(option)
            print(f'{option_dict=}')
            setattr(args, arg_name, option_dict)
        else:
            setattr(args, arg_name, dict())

    set_dict_arg_from_string(args.override_extractor, 'extraction_overrides')
    set_dict_arg_from_string(args.override_exporter, 'export_overrides')

    set_dict_arg_from_string(args.override_reader, 'reader_overrides')
    set_dict_arg_from_string(args.override_plot, 'plot_overrides')

    return args


def setup(options):
    # verbose printing of DataFrames
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)

    if options.slurm:
        print('using SLURM cluster')
        cluster = SLURMCluster(cores = 1
                             , n_workers = options.worker
                             # , n_workers = 1
                             # , processes = options.worker
                             # , processes = 1
                             , memory = "1GB"
                             , account = "dask_test"
                             # , queue = "normal"
                             , job_extra_directives = [ f'--nodelist={options.nodelist}' ]
                             , interface = 'lo'
                             , shared_temp_directory = options.tmpdir
                             )
        return Client(cluster)
    elif options.cluster:
        if options.cluster == 'local':
            print('using local process cluster')
            cluster = LocalCluster(n_workers=options.worker
                                 , host='localhost'
                                 # , interface='lo'
                                 , local_directory = options.tmpdir
                                 )
            cluster.scale(options.worker)
            return Client(cluster)
        else:
            print(f'using distributed cluster at {options.cluster}')
            client = Client(options.cluster)
            return client



def main():
    options = parse_args()
    print(f'{options=}')

    client = setup(options)

    process_recipe(options)
    print(globals())




if __name__=='__main__':
    main()

