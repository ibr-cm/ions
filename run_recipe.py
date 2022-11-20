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
        if options.run_tree and not extractor_name in options.run_tree['evaluation']['extractors']:
            print(f'skipping extractor {extractor_name}')
            continue
        extractor = recipe.evaluation.extractors[extractor_name]

        if extractor_name in options.extraction_overrides:
            extractor.input_files = [ options.extraction_overrides[extractor_name] ]
            print(f'overriding {extractor_name} with {extractor.input_files}')

        delayed_data = extractor.prepare()
        # print(f'{extractor=}')
        # print(f'{delayed_data.memory_usage(deep=True) = }')
        # print(f'-<-<-<-<-<-<-')
        data_repo[extractor_name] = delayed_data

        print(f'added extractor {extractor_name}')

    if not hasattr(recipe.evaluation, 'transforms'):
        print('execute_evaluation_phase: no `transforms` in recipe.Evaluation')
        return data_repo

    for transform_name in recipe.evaluation.transforms:
        if options.run_tree and not transform_name in options.run_tree['evaluation']['transforms']:
            print(f'skipping transform {transform_name}')
            continue
        transform = recipe.evaluation.transforms[transform_name]
        transform.set_data_repo(data_repo)
        transform.execute()
        print(f'added transform {transform_name}')

    jobs = []

    if recipe.evaluation.exporter is None:
        print('execute_evaluation_phase: no `exporter` in recipe.Evaluation')
        return data_repo

    for exporter_name in recipe.evaluation.exporter:
        if options.run_tree and not exporter_name in options.run_tree['evaluation']['exporter']:
            print(f'skipping exporter {exporter_name}')
            continue
        exporter = recipe.evaluation.exporter[exporter_name]

        if exporter_name in options.export_overrides:
            exporter.output_filename = options.export_overrides[exporter_name]
            print(f'overriding {exporter_name} with {exporter.output_filename}')

        exporter.set_data_repo(data_repo)
        job = exporter.execute()
        jobs.extend(job)
        print(f'added exporter {exporter_name}')


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
        if options.run_tree and not dataset_name in options.run_tree['plot']['reader']:
            print(f'skipping exporter {dataset_name}')
            continue
        reader = recipe.plot.reader[dataset_name]
        print(f'plot: loading dataset: "{dataset_name=}"')
        if dataset_name in options.reader_overrides:
            reader.input_files = options.reader_overrides[dataset_name]
            print(f'plot: execute_plotting_phase overriding input files for "{dataset_name}": "{reader.input_files=}"')
        data = reader.read_data()
        data_repo[dataset_name] = data
        print(f'added reader {dataset_name}')

    print('<<<-<-<--<-<-<--<-<-<')
    print(f'plot: {data_repo=}')
    print('<<<-<-<--<-<-<--<-<-<')

    for task_name in recipe.plot.transforms:
        if options.run_tree and not task_name in options.run_tree['plot']['transforms']:
            print(f'skipping transform {task_name}')
            continue
        task = recipe.plot.transforms[task_name]
        task.set_data_repo(data_repo)
        task.execute()
        print(f'added transform {task_name}')

    jobs = []
    for task_name in recipe.plot.tasks:
        if options.run_tree and not task_name in options.run_tree['plot']['tasks']:
            print(f'skipping task {task_name}')
            continue
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
        print(f'added task {task_name}')

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
    parser.add_argument('--run', type=str, default='all', help='run selected tasks/steps only') #TODO:description

    parser.add_argument('--worker', type=int, default=4, help='the number of worker processes')

    parser.add_argument('--cluster', type=str, help='cluster address')
    parser.add_argument('--single-threaded', action='store_true', default=False, help='run singlethreaded')

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


    def get_run_tree(option, phase_names):
        d = dict()
        d[phase_names[0]] = set()
        d[phase_names[1]] = set()
        d[phase_names[2]] = set()
        for qualified_task_name in option.split(','):
            if len(qualified_task_name) == 0:
                continue
            top_level, task_name = qualified_task_name.split('.')
            d[top_level] = d[top_level].union(set([task_name]))
        return d

    def process_run_tree(option):
        eval_phases = ['extractors', 'transforms', 'exporter']
        plot_phases = ['reader', 'transforms', 'tasks']

        p = option.split(':')
        if len(p) == 1 or (len(p) == 2 and len(p[1]) == 0):
            # evaluation phase only
            eval_str = p[0]
            plot_str = ''
        else:
            eval_str = p[0]
            plot_str = p[1]

        run_tree = { 'evaluation': get_run_tree(eval_str, eval_phases)
                    , 'plot': get_run_tree(plot_str, plot_phases)
                    }
        return run_tree

    if args.run != 'all':
        run_tree = process_run_tree(args.run)
    else:
        run_tree = None

    setattr(args, 'run_tree', run_tree)
    print(f'{run_tree=}')

    return args


def setup_dask(options):
    # verbose printing of DataFrames
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)

    # single-threaded mode for debugging
    if options.single_threaded:
        print('using local single-threaded process cluster')
        dask.config.set(scheduler='synchronous')
        # no client is returned, creating a client here leads to sqlite
        # connections objects being transported between threads
        return None

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
    else:
        print(f'using local cluster with dashboard at localhost:8787')
        client = Client(dashboard_address='localhost:8787')
        return client


def main():
    options = parse_args()
    print(f'{options=}')

    client = setup_dask(options)

    process_recipe(options)
    print(globals())




if __name__=='__main__':
    main()

