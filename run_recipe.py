#!/usr/bin/python3

import pprint
import sys
import os
import shutil
import argparse
import traceback
import threading

from typing import Callable

# ---

import logging


# ---

import yaml

from yaml import dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

import pandas as pd

import numexpr

import dask
import dask.distributed
from dask.distributed import Client, LocalCluster
from dask_jobqueue import SLURMCluster


# ---

from common.logging_facilities import logi, loge, logd, setup_logging_defaults, set_logging_level

from recipe import Recipe

import extractors
import transforms
import exporters
import plots

# ---

import tag_regular_expressions as tag_regex

# import debug helper for usage in function definitions
from common.debug import start_debug # noqa: F401recipe import Recipe

from utility.code import compile_and_evaluate_function_definition

_debug = False


def evaluate_function_definitions(recipe:Recipe):
    r"""
    Evaluate the functions defined in `recipe.function_definitions` and make the compiled code available in a copy of
    the global environment of the interpreter.
    A copy of the global environment is used so as to not pollute the global namespace itself. All the defined functions share the same copy.

    Parameters
    ----------
    recipe: Recipe
        The recipe in which the functions are to be made available.

    Returns
    -------
        A shallow copy of the global environment with the compiled functions as members, i.e. a dictionary with the
        function names as key and the code as associated value.
    """
    # Create a copy of the global environment for evaluating the extra code fragments so as to not pollute the global
    # namespace itself.
    #
    # NOTE: This is a shallow copy and thus a rather simple method to prevent accidental overwrites, *not* a defense
    # against deliberately malicious modifications.
    global_env = globals().copy()

    for function_name in recipe.function_definitions:
        # Get the string with the source code.
        function_code = recipe.function_definitions[function_name]
        # Actually evaluate the code within the given namespace to allow
        # access to all the defined symbols, such as helper functions that are not defined inline.
        function, global_env = compile_and_evaluate_function_definition(function_code, function_name, global_env)
        # Bind the function to the specified name.
        recipe.function_definitions[function_name] = function

    # Return the environment with the compiled functions.
    return global_env


def eval_recipe_tag_definitions(recipe:Recipe
                                , attributes_regex_map, iterationvars_regex_map, parameters_regex_map
                                , function_definitions_global_env:dict
                               ):
    def eval_and_add_tags(tag_set_name, regex_map):
        for tag_name in recipe.evaluation.tags[tag_set_name]:
            # The `eval` is necessary here since the `transform` function of the tag
            # can be an arbitrary function and has to be parsed into a`Callable`.
            tag_list = eval(recipe.evaluation.tags[tag_set_name][tag_name], function_definitions_global_env) # pylint: disable=W0123:eval-used

            # Check that the transform is indeed a `Callable` .
            for tag in tag_list:
                if not isinstance(tag['transform'], Callable):
                    raise RuntimeError(f'transform for {tag=} is not a Callable!')

            regex_map[tag_name] = tag_list

    if 'attributes' in recipe.evaluation.tags:
        eval_and_add_tags('attributes', attributes_regex_map)
    if 'iterationvars' in recipe.evaluation.tags:
        eval_and_add_tags('iterationvars', iterationvars_regex_map)
    if 'parameters' in recipe.evaluation.tags:
        eval_and_add_tags('parameters', parameters_regex_map)

    return attributes_regex_map, iterationvars_regex_map, parameters_regex_map


def prepare_evaluation_phase(recipe:Recipe, options, data_repo, function_definitions_global_env:dict):
    logi(f'prepare_evaluation_phase: {recipe}  {recipe.name}')

    if hasattr(recipe.evaluation, 'tags'):
        attributes_regex_map, iterationvars_regex_map, parameters_regex_map = eval_recipe_tag_definitions(recipe \
                , tag_regex.attributes_regex_map, tag_regex.iterationvars_regex_map, tag_regex.parameters_regex_map
                , function_definitions_global_env)
    else:
        attributes_regex_map, iterationvars_regex_map, parameters_regex_map = \
                tag_regex.attributes_regex_map, tag_regex.iterationvars_regex_map, tag_regex.parameters_regex_map


    if not hasattr(recipe.evaluation, 'extractors'):
        logi('prepare_evaluation_phase: no `extractors` in recipe.Evaluation')
        return

    for extractor_tuple in recipe.evaluation.extractors:
        extractor_name = list(extractor_tuple.keys())[0]
        extractor = list(extractor_tuple.values())[0]

        if options.run_tree and extractor_name not in options.run_tree['evaluation']['extractors']:
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

            if options.run_tree and transform_name not in options.run_tree['evaluation']['transforms']:
                logi(f'skipping transform {transform_name}')
                continue
            logi(f'preparing transform {transform_name}')
            transform.set_name(transform_name)
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

            if options.run_tree and exporter_name not in options.run_tree['evaluation']['exporter']:
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

            if options.run_tree and dataset_name not in options.run_tree['plot']['reader']:
                logi(f'skipping reader {dataset_name}')
                continue
            logi(f'plot: loading dataset: "{dataset_name=}"')
            if dataset_name in options.reader_overrides:
                reader.input_files = options.reader_overrides[dataset_name]
                logi(f'plot: prepare_plotting_phase overriding input files for "{dataset_name}": "{reader.input_files=}"')
            data = reader.prepare()
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

            if options.run_tree and transform_name not in options.run_tree['plot']['transforms']:
                logi(f'skipping transform {transform_name}')
                continue
            transform.set_name(transform_name)
            transform.set_data_repo(data_repo)
            transform.prepare()
            logi(f'added transform {transform_name}')

    jobs = []
    for task_tuple in recipe.plot.tasks:
        task_name = list(task_tuple.keys())[0]
        task = list(task_tuple.values())[0]

        if options.run_tree and task_name not in options.run_tree['plot']['tasks']:
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

    if options.dump_recipe or options.dump_recipe_only:
        output = dump(recipe, Dumper=Dumper)
        terminal_size = shutil.get_terminal_size()
        logd(pprint.pformat(output, width=terminal_size.columns))
        if options.dump_recipe_only:
            exit()

    data_repo = {}
    job_list = []

    # Compile all the functions defined in `function_definitions` and make them available in a shallow copy
    # of the runtime environment.
    if hasattr(recipe, 'function_definitions'):
        function_definitions_global_env = evaluate_function_definitions(recipe)
    else:
        # Or just use the default environment.
        function_definitions_global_env = globals()

    if not options.plot_only:
        if not hasattr(recipe, 'evaluation'):
            logi('process_recipe: no Evaluation in recipe')
            return
        data_repo, jobs = prepare_evaluation_phase(recipe, options, data_repo, function_definitions_global_env)
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
    parser.add_argument('recipe', help='Input recipe')

    parser.add_argument('--override-extractor', type=str, help='Override extractor parameters')
    parser.add_argument('--override-exporter', type=str, help='Override exporter parameters')

    parser.add_argument('--override-reader', type=str, help='Override reader parameters')
    parser.add_argument('--override-plot', type=str, help='Override plot parameters')

    parser.add_argument('--eval-only', action='store_true', default=False, help='Run evaluation phase only')
    parser.add_argument('--plot-only', action='store_true', default=False, help='Run plot phase only')
    parser.add_argument('--run', type=str, default='all', help='Run selected tasks only, in the format [evaluation_phase_tasks]:[plot_phase_tasks]'
                                                                ' where each (optional) phase consists of a comma-separated list of qualified task names,'
                                                                ' i.e. the name of the sub-phase and the task name'
                                                                ' e.g. `extractors.e1,transforms.t1,exporter.e1:reader.r1,transforms.t1,tasks.plot1`'
                                                                )

    parser.add_argument('--worker', type=int, default=4, help='The number of worker processes')
    parser.add_argument('--mem', type=int, default=1, help='The memory, in GB, to reserve for each worker process')

    parser.add_argument('--cluster', type=str, help='The address of an already running cluster')
    parser.add_argument('--single-threaded', action='store_true', default=False, help='Run in single-threaded mode; this overrides the value of the `--worker` flag')
    parser.add_argument('--dashboard-port', type=int, default=8787, help='The port for the dashboard of the cluster. For the default localhost cluster')

    parser.add_argument('--slurm', action='store_true', default=False, help='Use a SLURM cluster')
    parser.add_argument('--partition', type=str, help='The partition for the SLURM cluster')
    parser.add_argument('--nodelist', type=str, help='The nodelist for the SLURM cluster')

    parser.add_argument('--tmpdir', type=str, default='/opt/tmpssd/tmp', help='The directory for temporary files')

    parser.add_argument('--plot-task-graphs', action='store_true', default=False, help='Plot the evaluation and plotting phase task graph')

    parser.add_argument('--verbose', '-v', action='count', default=0, help='Increase logging verbosity')

    parser.add_argument('--dump-recipe', action='store_true', default=False, help='Dump the loaded recipe; useful for finding errors in the recipe')
    parser.add_argument('--dump-recipe-only', action='store_true', default=False, help='Dump the loaded recipe and exit; useful for finding errors in the recipe')

    parser.add_argument('--debug', action='store_true', default=False, help='Enable debug mode. If an exception is encountered, drop into an ipdb debugger session')


    args = parser.parse_args(arguments)

    if args.debug:
        global _debug
        _debug = True

    if args.single_threaded:
        args.worker = 1

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

    # print the numexpr thread pool config to the log
    numexp_max_threads = os.getenv('NUMEXPR_MAX_THREADS')
    numexp_num_threads = os.getenv('NUMEXPR_NUM_THREADS')

    numexpr_threads_log_msg = f'thread_id={threading.get_native_id()}'
    if numexp_max_threads:
        numexpr_threads_log_msg += f'  NUMEXPR_MAX_THREADS={numexp_max_threads}'
    if numexp_num_threads:
        numexpr_threads_log_msg += f'  NUMEXPR_NUM_THREADS={numexp_num_threads}'

    numexpr_threads_log_msg += f'  {numexpr.ncores=}  {numexpr.nthreads=}  {numexpr.MAX_THREADS=}'
    logd(numexpr_threads_log_msg)

class WorkerPlugin(dask.distributed.WorkerPlugin):
    r"""
    A dask worker plugin for setting defaults in the worker process

    Parameters
    ----------
    options : dict
        The dictionary containing the configuration for the worker, usually the same as for the launcher
    """
    def __init__(self, options, *args, **kwargs):
        self.options = options

    def setup(self, worker: dask.distributed.Worker):
        # append the current path to the PYTHONPATH of the worker
        sys.path.append('.')
        setup_logging_defaults(level=self.options.log_level)
        set_logging_level(self.options.log_level)
        setup_pandas()


def setup_dask(options):
    r"""
    Setup and configure the dask cluster and its workers.

    Parameters
    ----------
    options : dict
        The dictionary containing the configuration for the launcher
    """
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
        dashboard_address = f'localhost:{options.dashboard_port}'
        logi(f'using local cluster with dashboard at {dashboard_address}')
        client = Client(dashboard_address=dashboard_address, n_workers=options.worker)
        client.register_worker_plugin(plugin)
        return client


def compute_graph(jobs):
    r"""
    Compute the task graph

    Parameters
    ----------
    jobs : List[dask.Delayed]
        The list of jobs/tasks to compute
    """
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

    # This is needed for proper pickling of DataFrames to JSON.
    exporters.register_jsonpickle_handlers()

    # register constructors for all YAML objects
    extractors.register_constructors()
    transforms.register_constructors()
    exporters.register_constructors()
    plots.register_constructors()

    data_repo, job_list = process_recipe(options)

    if len(job_list) == 0:
        loge('No tasks to run')
        return

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

        if sys.stdin.isatty() and _debug:
            loge('dropping into an interactive debugging environment')
            # drop into the debugger in the context of the exception thrown
            import ipdb
            ipdb.post_mortem(e.__traceback__)
        else:
            loge('not dropping into an interactive debugging environment since the executing interpreter is not interactive')
