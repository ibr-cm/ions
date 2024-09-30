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
# TODO: quick & dirty
def load_all_inputs(input_filenames):
    input_list = []
    for filename in input_filenames:
        try:
            df = read_from_file(filename)
            input_list.append(df)
        except FileNotFoundError as e:
            print(f'!!>> File "{filename}" not found:\n {e}')
            continue

    return input_list

def verify_equality(df0, df1):
    is_equal = all(df0, df1)
    return is_equal

# TODO: quick & dirty, comparison only works for two inputs
def process_multiple_inputs(input_list:Iterable[str]
                            , context_options
                            , compare:bool = False
                            , detailed_compare:bool = False
                            ):
    dfs = load_all_inputs(input_list)

    if compare or detailed_compare:
        if detailed_compare:
            res = dfs[0].compare(dfs[1])
            print(f'{res=}')
            is_equal = all((dfs[0] == dfs[1]).all())
            equal_result = '' if is_equal else ' *NOT*'
        else:
            is_equal = all((dfs[0] == dfs[1]).all())
            equal_result = '' if is_equal else ' *NOT*'

        print(f'{is_equal}: values in the given DataFrames are{equal_result} equal')

        return is_equal
    else:
        with pd.option_context(*context_options):
            print('>>>> Starting a IPython shell...\n>>>> The data has been loaded into `dfs`')
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
        process_multiple_inputs(args.input, context_options, compare=args.compare, detailed_compare=args.detailed_compare)
    else:
        process_single_input(args.input[0], context_options)


if __name__=='__main__':
    try:
        main()
    except Exception as e:
        print(f'Encountered error: {e}')
        print(''.join(traceback.format_exception(e)))
