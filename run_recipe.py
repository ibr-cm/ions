#!/usr/bin/python3

import pprint
import sys
import argparse
import traceback

from typing import Callable

# ---

import logging

from common.logging_facilities import logi, loge, logd, logw \
                                        , setup_logging_defaults, set_logging_level

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

from recipe import Recipe

import extractors
import transforms
import exporters
import plots

from sql_queries import generate_signal_query

# ---

import tag_regular_expressions as tag_regex


def eval_recipe_tag_definitions(recipe, attributes_regex_map, iterationvars_regex_map, parameters_regex_map):
    def eval_and_add_tags(tag_set_name, regex_map):
        for tag_name in recipe.evaluation.tags[tag_set_name]:
            tag_list = eval(recipe.evaluation.tags[tag_set_name][tag_name])
            logd(f'{tag_name=} {tag_list=}')
            l = []
            for tag in tag_list:
                if not isinstance(tag['transform'], Callable):
                    tag['transform'] = eval(tag['transform'])
                l.append(tag)

            regex_map[tag_name] = l

    if 'attributes' in recipe.evaluation.tags:
        eval_and_add_tags('attributes', attributes_regex_map)
    if 'iterationvars' in recipe.evaluation.tags:
        eval_and_add_tags('iterationvars', iterationvars_regex_map)
    if 'parameters' in recipe.evaluation.tags:
        eval_and_add_tags('parameters', parameters_regex_map)

    return attributes_regex_map, iterationvars_regex_map, parameters_regex_map


def prepare_evaluation_phase(recipe:Recipe, options, data_repo):
    logi(f'prepare_evaluation_phase: {recipe}  {recipe.name}')

    if hasattr(recipe.evaluation, 'tags'):
        attributes_regex_map, iterationvars_regex_map, parameters_regex_map = eval_recipe_tag_definitions(recipe \
                , tag_regex.attributes_regex_map, tag_regex.iterationvars_regex_map, tag_regex.parameters_regex_map)
    else:
        attributes_regex_map, iterationvars_regex_map, parameters_regex_map = \
                tag_regex.attributes_regex_map, tag_regex.iterationvars_regex_map, tag_regex.parameters_regex_map


    if not hasattr(recipe.evaluation, 'extractors'):
        logi('prepare_evaluation_phase: no `extractors` in recipe.Evaluation')
        return

    for extractor_tuple in recipe.evaluation.extractors:
        extractor_name = list(extractor_tuple.keys())[0]
        extractor = list(extractor_tuple.values())[0]

        if options.run_tree and not extractor_name in options.run_tree['evaluation']['extractors']:
            logi(f'skipping extractor {extractor_name}')
            continue

        if extractor_name in options.extraction_overrides:
            extractor.input_files = [ options.extraction_overrides[extractor_name] ]
            logi(f'overriding {extractor_name} with {extractor.input_files}')

        extractor.set_tag_maps(attributes_regex_map, iterationvars_regex_map, parameters_regex_map)

        delayed_data = extractor.prepare()
        # print(f'{extractor=}')
        # print(f'{delayed_data.memory_usage(deep=True) = }')
        # print(f'-<-<-<-<-<-<-')
        data_repo[extractor_name] = delayed_data
        logi(f'added extractor {extractor_name}')

    if not hasattr(recipe.evaluation, 'transforms'):
        logi('prepare_evaluation_phase: no `transforms` in recipe.Evaluation')
    else:
        for transform_tuple in recipe.evaluation.transforms:
            transform_name = list(transform_tuple.keys())[0]
            transform = list(transform_tuple.values())[0]

            if options.run_tree and not transform_name in options.run_tree['evaluation']['transforms']:
                logi(f'skipping transform {transform_name}')
                continue
            logi(f'preparing transform {transform_name}')
            transform.set_data_repo(data_repo)
            transform.prepare()

            logi(f'added transform {transform_name}')

    jobs = []

    if recipe.evaluation.exporter is None:
        logi('prepare_evaluation_phase: no `exporter` in recipe.Evaluation')
    else:
        for exporter_tuple in recipe.evaluation.exporter:
            exporter_name = list(exporter_tuple.keys())[0]
            exporter = list(exporter_tuple.values())[0]

            if options.run_tree and not exporter_name in options.run_tree['evaluation']['exporter']:
                logi(f'skipping exporter {exporter_name}')
                continue

            if exporter_name in options.export_overrides:
                exporter.output_filename = options.export_overrides[exporter_name]
                logi(f'overriding {exporter_name} with {exporter.output_filename}')

            exporter.set_data_repo(data_repo)
            job = exporter.prepare()
            jobs.extend(job)
            logi(f'added exporter {exporter_name}')


    logi(f'{jobs=}')

    if options.plot_task_graphs:
        for i in range(0, len(jobs)):
            jobs[i].visualize(f'{options.tmpdir}/dask_task_graph_evaluation_job-{i}.png')

    return data_repo, jobs


def prepare_plotting_phase(recipe:Recipe, options, data_repo):
    logi(f'prepare_plotting_phase: {recipe}  {recipe.name}')

    if not hasattr(recipe.plot, 'reader'):
        logi('prepare_plotting_phase: no `reader` in recipe.Plot')
    else:
        for dataset_tuple in recipe.plot.reader:
            dataset_name = list(dataset_tuple.keys())[0]
            reader = list(dataset_tuple.values())[0]

            if options.run_tree and not dataset_name in options.run_tree['plot']['reader']:
                logi(f'skipping reader {dataset_name}')
                continue
            logi(f'plot: loading dataset: "{dataset_name=}"')
            if dataset_name in options.reader_overrides:
                reader.input_files = options.reader_overrides[dataset_name]
                logi(f'plot: prepare_plotting_phase overriding input files for "{dataset_name}": "{reader.input_files=}"')
            data = reader.read_data()
            data_repo[dataset_name] = data
            logi(f'added reader {dataset_name}')

    logd('<<<-<-<--<-<-<--<-<-<')
    logd(f'plot: {data_repo=}')
    logd('<<<-<-<--<-<-<--<-<-<')

    if not hasattr(recipe.plot, 'transforms'):
        logi('prepare_plotting_phase: no `transforms` in recipe.Plot')
    else:
        for transform_tuple in recipe.plot.transforms:
            transform_name = list(transform_tuple.keys())[0]
            transform = list(transform_tuple.values())[0]

            if options.run_tree and not transform_name in options.run_tree['plot']['transforms']:
                logi(f'skipping transform {transform_name}')
                continue
            transform.set_data_repo(data_repo)
            transform.prepare()
            logi(f'added transform {transform_name}')

    jobs = []
    for task_tuple in recipe.plot.tasks:
        task_name = list(task_tuple.keys())[0]
        task = list(task_tuple.values())[0]

        if options.run_tree and not task_name in options.run_tree['plot']['tasks']:
            logi(f'skipping task {task_name}')
            continue

        if task_name in options.plot_overrides:
            task.output_file = options.plot_overrides[task_name]
            logi(f'overriding {task_name} with {task.output_file}')

        task.set_data_repo(data_repo)
        logi(f'plot: preparing plotting task {task_name}')
        job = task.prepare()
        # logi(f'plot: {job=}')
        jobs.append(job)
        logi(f'added task {task_name}')

    if options.plot_task_graphs:
        for i in range(0, len(jobs)):
            graph_output_file = f'{options.tmpdir}/dask_task_graph_plotting_job-{i}.png'
            jobs[i].visualize(graph_output_file)
            logi(f'saved the plot phase task graph for job {jobs[i]} to: {graph_output_file}')

    return data_repo, jobs


def process_recipe(options):
    f = open(options.recipe, mode='r')

    recipe = yaml.unsafe_load(f.read())

    if options.dump_recipe:
        output = dump(recipe, Dumper=Dumper)
        logd(pprint.pformat(output, width=192))
        exit()

    data_repo = {}
    job_list = []

    if not options.plot_only:
        if not hasattr(recipe, 'evaluation'):
            logi('process_recipe: no Evaluation in recipe')
            return
        data_repo, jobs = prepare_evaluation_phase(recipe, options, data_repo)
        job_list.extend(jobs)

    if options.eval_only:
        return data_repo, job_list

    if not hasattr(recipe, 'plot'):
        logi('process_recipe: no Plot in recipe')
        return data_repo, job_list

    data_repo, jobs = prepare_plotting_phase(recipe, options, data_repo)
    job_list.extend(jobs)

    return data_repo, job_list


def extract_dict_from_string(string):
    d = dict()
    for token in string.split(','):
        key, value = token.strip(',').split(':')
        d[key] = value
    return d


def parse_arguments(arguments):
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('recipe', help='input recipe')

    parser.add_argument('--override-extractor', type=str, help='override extractor parameters')
    parser.add_argument('--override-exporter', type=str, help='override exporter parameters')

    parser.add_argument('--override-reader', type=str, help='override reader parameters')
    parser.add_argument('--override-plot', type=str, help='override plot parameters')

    parser.add_argument('--eval-only', action='store_true', default=False, help='run eval phase only')
    parser.add_argument('--plot-only', action='store_true', default=False, help='run plot phase only')
    parser.add_argument('--run', type=str, default='all', help='run selected tasks/steps only') #TODO:description

    parser.add_argument('--worker', type=int, default=4, help='the number of worker processes')
    parser.add_argument('--mem', type=int, default=1, help='the memory, in GB, to reserve for each worker process')

    parser.add_argument('--cluster', type=str, help='cluster address')
    parser.add_argument('--single-threaded', action='store_true', default=False, help='run singlethreaded')

    parser.add_argument('--slurm', action='store_true', default=False, help='use SLURM cluster')
    parser.add_argument('--partition', type=str, help='partition for SLURM')
    parser.add_argument('--nodelist', type=str, help='nodelist for SLURM')

    parser.add_argument('--tmpdir', type=str, default='/opt/tmpssd/tmp', help='directory for temporary files')

    parser.add_argument('--plot-task-graphs', action='store_true', default=False, help='plot the evaluation and plotting phase task graph')

    parser.add_argument('--verbose', '-v', action='count', default=0, help='increase logging verbosity')

    parser.add_argument('--dump-recipe', action='store_true', default=False, help='dump the loaded recipe, useful for finding errors in the recipe')

    args = parser.parse_args(arguments)

    if args.slurm:
        if not args.nodelist:
            raise Exception('A nodelist ist required when using SLURM.')
        if not args.partition:
            raise Exception('A partition ist required when using SLURM.')

    def set_dict_arg_from_string(option, arg_name):
        if option:
            try:
                option_dict = extract_dict_from_string(option)
            except Exception as e:
                loge(f'>>>> ERROR: extracting parameters for `{arg_name}` from `{option}` failed:\n>>>> {e}')
                return
            logd(f'{option_dict=}')
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
            if len(r := qualified_task_name.split('.')) == 2:
                top_level, task_name = r
            else:
                loge(f'>>>> ERROR: bad task name: {qualified_task_name}')
                exit(1)
            if top_level in d:
                d[top_level] = d[top_level].union(set([task_name]))
            else:
                loge(f'>>>> ERROR: not a valid phase name: {top_level}')
                exit(1)
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
    logd(f'{run_tree=}')


    def map_verbosity_level_to_log_level(verbosity):
        match verbosity:
            case 0:
                log_level = logging.WARNING
            case 1:
                log_level = logging.INFO
            case 2:
                log_level = logging.DEBUG
            case 3:
                log_level = logging.NOTSET
            case _:
                log_level = logging.NOTSET
        return log_level

    log_level = map_verbosity_level_to_log_level(args.verbose)
    setattr(args, 'log_level', log_level)

    return args


def setup_pandas():
    # verbose printing of DataFrames
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)


class WorkerPlugin(dask.distributed.WorkerPlugin):
    def __init__(self, options, *args, **kwargs):
        self.options = options

    def setup(self, worker: dask.distributed.Worker):
        # append the current path to the PYTHONPATH of the worker
        sys.path.append('.')
        setup_logging_defaults(level=self.options.log_level)
        setup_pandas()


def setup_dask(options):
    plugin = WorkerPlugin(options)

    dask.config.set({'distributed.scheduler.worker-ttl': None})

    # single-threaded mode for debugging
    if options.single_threaded:
        logi('using local single-threaded process cluster')
        dask.config.set(scheduler='synchronous')
        # no client is returned, creating a client here leads to sqlite
        # connections objects being transported between threads
        return None

    if options.slurm:
        logi('using SLURM cluster')
        cluster = SLURMCluster(cores = 1
                             , n_workers = options.worker
                             , memory = str(options.mem) + 'GB'
                             , job_extra_directives = [ f'--nodelist={options.nodelist} --partition={options.partition}' ]
                             , interface = 'lo'
                             , shared_temp_directory = options.tmpdir
                             )
        client = Client(cluster)
        client.register_worker_plugin(plugin)
        return client
    elif options.cluster:
        if options.cluster == 'local':
            logi('using local process cluster')
            cluster = LocalCluster(n_workers=options.worker
                                 , host='localhost'
                                 # , interface='lo'
                                 , local_directory = options.tmpdir
                                 )
            cluster.scale(options.worker)
            client = Client(cluster)
            client.register_worker_plugin(plugin)
            return client
        else:
            logi(f'using distributed cluster at {options.cluster}')
            client = Client(options.cluster)
            client.register_worker_plugin(plugin)
            return client
    else:
        logi(f'using local cluster with dashboard at localhost:8787')
        client = Client(dashboard_address='localhost:8787', n_workers=options.worker)
        client.register_worker_plugin(plugin)
        return client


def compute_graph(jobs):
    logi('=-!!'*40)
    logi('recombobulating splines...')
    logi(f'compute_graph: {jobs=}')
    result = dask.compute(*jobs)
    logi('=-!!'*40)
    return result

def main():
    setup_logging_defaults(logging.WARNING)

    options = parse_arguments(sys.argv[1:])

    # setup logging level again
    set_logging_level(options.log_level)

    logd(f'{options=}')

    setup_pandas()

    client = setup_dask(options)

    # register constructors for all YAML objects
    extractors.register_constructors()
    transforms.register_constructors()
    exporters.register_constructors()
    plots.register_constructors()

    data_repo, job_list = process_recipe(options)

    # now actually compute the constructed computation graph
    result = compute_graph(job_list)

    # ...
    return

if __name__=='__main__':
    try:
        main()
    except Exception as e:
        loge(f'{e=}')
        loge(''.join(traceback.format_exception(e)))

