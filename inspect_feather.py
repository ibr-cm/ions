#!/usr/bin/env python3

import time
import argparse
import traceback
import errno

from typing import Optional, Iterable

import pyarrow

import pandas as pd

from IPython.display import display
from IPython import embed

from data_io import read_from_file

########################################
# TODO: quick & dirty, better to use `FileResultProcessor`
# (or extract the `save` function from `FileResultProcessor` to `data.io.py`
# and use that)
def save(df:pd.DataFrame, path:str, compression='lz4'):
    try:
        df.to_feather(path, compression=compression)
    except Exception as e:
        print(f'Error occured while trying to save DataFrame to {path}\n{e}')
        raise e


########################################

def set_environment_defaults(num_threads=4):
    import numexpr
    numexpr.set_num_threads(num_threads)

    import pyarrow
    pyarrow.set_cpu_count(num_threads)


def display_full(df, max_rows:Optional[int] = None, max_columns:Optional[int] = None
                 , max_colwidth:Optional[float] = None
                 , precision:int = pd.options.display.precision
                 , width:int = 1000):
    with pd.option_context('display.max_rows', max_rows, 'display.max_columns', max_columns
                          , 'display.max_colwidth', max_colwidth , 'display.width', width
                          , 'display.precision', precision
                          ):
        display(df)

#######################
# TODO: Quick & dirty. This could be parallelized.
def load_all_inputs(input_filenames):
    input_list = []
    loaded_filenames = []
    for filename in input_filenames:
        try:
            df = read_from_file(filename)
            input_list.append(df)
            loaded_filenames.append(filename)
        except FileNotFoundError as e:
            print(f'!!>> File "{filename}" not found:\n {e}')
            continue

    return input_list, loaded_filenames

def is_df_pair_equal(df0, df1):
    is_equal = all((df0 == df1).all())

    return is_equal

def is_df_pair_equal_detailed(df0, df1):
    diff = df0.compare(df1)
    is_equal = diff.empty

    return is_equal, diff

def process_multiple_inputs(input_list:Iterable[str]
                            , context_options
                            , compare:bool = False
                            , detailed_compare:bool = False
                            ):
    dfs, loaded_filenames = load_all_inputs(input_list)

    if compare or detailed_compare:
        is_equal_results:list[bool] = []
        num_dfs:int = len(dfs)
        diffs:dict[int, pd.DataFrame] = {}

        # Compare the loaded DataFrames one-to-one sequencially.
        if detailed_compare:
            for i in range(0, num_dfs-1):
                print(f'comparing {loaded_filenames[i]} with {loaded_filenames[i+1]}')
                is_equal, diff = is_df_pair_equal_detailed(dfs[i], dfs[i+1])
                # Keep the differenve for the detailed log report.
                if not diff.empty:
                    diffs[i] = diff
                is_equal_results.append(is_equal)
        else:
            for i in range(0, num_dfs-1):
                print(f'comparing {loaded_filenames[i]} with {loaded_filenames[i+1]}')
                is_equal = is_df_pair_equal(dfs[i], dfs[i+1])
                is_equal_results.append(is_equal)

        is_equal = all(is_equal_results)
        equal_result_str = "" if is_equal else " *NOT*"
        print(f'{is_equal}: values in the given DataFrames are{equal_result_str} equal')

        if detailed_compare:
            # Print the differences for unequal DataFrames.
            for k in diffs:
                print(f'{k}: {loaded_filenames[k]}: {loaded_filenames[k+1]} :\n{diffs[k]}')

        return is_equal
    else:
        with pd.option_context(*context_options):
            print('>>>> Starting a IPython shell...\n>>>> The DataFrames has been loaded into `dfs`')
            embed(color_info=True, colors='Linux')

#######################

def process_context_options(args):
    format_string = '{:0.' + str(args.precision) +'f}'
    # print(f'{format_string=}')
    context_options = ['display.precision', args.precision, 'display.float_format', format_string.format]
    return context_options

def process_single_input(input_file, context_options, full:bool = False, query:Optional[str] = None):
    try:
        df = read_from_file(input_file)
    except FileNotFoundError as e:
        print(f'{e}\nFile "{input_file}" not found')
        exit(errno.ENOENT)

    print(f'{df.columns=}')

    default_columns = [ 'v2x_rate', 'repetition', 'prefix', 'moduleName', 'variable', 'ql', 'eventNumber', 'simtimeRaw', 'vectorName', 'value']
    columns = list(set(default_columns).intersection(df.columns))

    with pd.option_context(*context_options):
        if full:
            if query:
                display_full(df.query(query)[columns])
            else:
                # print('Starting a IPython shell...\nThe data has been loaded into `df`')
                # embed(color_info=True, colors='Linux')
                display_full(df[columns])
        else:
            if query:
                display(df.query(query)[columns])
            else:
                print('>>>> Starting a IPython shell...\n>>>> The data has been loaded into `df`')
                embed(color_info=True, colors='Linux')

#######################


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input', nargs='+')
    parser.add_argument('--threads', type=int, default=4)

    parser.add_argument('--full', action='store_true', required=False)
    parser.add_argument('--precision', type=int, default=pd.options.display.precision)
    parser.add_argument('--query', type=str, default=None)

    parser.add_argument('--compare', action='store_true', default=False, required=False)
    parser.add_argument('--detailed-compare', action='store_true', default=False, required=False)

    args = parser.parse_args()

    set_environment_defaults(args.threads)

    context_options = process_context_options(args)

    if len(args.input) > 1 :
        result_code = process_multiple_inputs(args.input, context_options, compare=args.compare, detailed_compare=args.detailed_compare)
        if result_code is not None:
            exit((0 if result_code else 1))
        else:
            exit(0)
    else:
        process_single_input(args.input[0], context_options)


if __name__=='__main__':
    try:
        main()
    except Exception as e:
        print(f'Encountered error: {e}')
        print(''.join(traceback.format_exception(e)))
