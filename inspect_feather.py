#!/usr/bin/env python3

import time
import argparse

from typing import Optional

import pyarrow

import pandas as pd

from IPython.display import display
from IPython import embed

from data_io import read_from_file

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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input')
    parser.add_argument('--threads', type=int, default=4)

    parser.add_argument('--full', action='store_true', required=False)
    parser.add_argument('--precision', type=int, default=pd.options.display.precision)
    parser.add_argument('--query', type=str, default=None)


    args = parser.parse_args()

    set_environment_defaults(args.threads)

    directory = '/'.join(args.input.split('/')[:-1])
    filename = args.input.split('/')[-1:][0]
    print(f'{directory=}   {filename=}')

    df = read_from_file(directory +'/'+ filename)

    print(f'{df.columns=}')

    default_columns = [ 'v2x_rate', 'repetition', 'prefix', 'moduleName', 'variable', 'ql', 'eventNumber', 'simtimeRaw', 'vectorName', 'value']
    columns = list(set(default_columns).intersection(df.columns))

    format_string = '{:0.' + str(args.precision) +'f}'
    # print(f'{format_string=}')
    context_options = ['display.precision', args.precision, 'display.float_format', format_string.format]
    with pd.option_context(*context_options):
        if args.full:
            if args.query:
                display_full(df.query(args.query)[columns])
            else:
                # print('Starting a IPython shell...\nThe data has been loaded into `df`')
                # embed(color_info=True, colors='Linux')
                display_full(df[columns])
        else:
            if args.query:
                display(df.query(args.query)[columns])
            else:
                print('>>>> Starting a IPython shell...\n>>>> The data has been loaded into `df`')
                embed(color_info=True, colors='Linux')


if __name__=='__main__':
    try:
        main()
    except Exception as e:
        print(f'Encountered error: {e}')
        print(''.join(traceback.format_exception(e)))
