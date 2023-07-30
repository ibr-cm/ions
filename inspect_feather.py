#!/usr/bin/env python3

import time
import argparse

import pyarrow

import pandas as pd

from IPython.display import display

from data_io import read_from_file

########################################

def set_environment_defaults(num_threads=4):
    import numexpr
    numexpr.set_num_threads(num_threads)

    import pyarrow
    pyarrow.set_cpu_count(num_threads)


def display_full(df):
    with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.max_colwidth', None):
        display(df)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input')
    parser.add_argument('--threads', type=int, default=4)

    parser.add_argument('--full', action='store_true', required=False)
    parser.add_argument('--precision', type=int, default=8)
    parser.add_argument('--query', type=str, default=None)

    args = parser.parse_args()

    set_environment_defaults(args.threads)

    directory = '/'.join(args.input.split('/')[:-1])
    filename = args.input.split('/')[-1:][0]
    print(f'{directory=}   {filename=}')

    df = read_from_file(directory +'/'+ filename)

    print(f'{df.columns=}')
    # print(f'{df=}')
    # print(f'{df[["repetition", "mean"]]=}')
    # print(f'{df["prefix"].unique()=}')


    # default_columns=['eventNumber', 'simtimeRaw', 'vectorName', 'value']
    default_columns=['repetition', 'moduleName', 'sender', 'specific_information_age', 'specific_reliability']
    # default_columns=['repetition', 'moduleName', 'sender', 'specific_reliability']

    format_string = '{:0.' + str(args.precision) +'f}'
    # print(f'{format_string=}')
    # with pd.option_context('display.precision', args.precision, 'display.float_format', format_string.format):
    with pd.option_context('display.float_format', format_string.format):
        if args.full:
            # display_full(df[default_columns].sort_values(by=['simtimeRaw', 'vectorName']))
            # display_full(df[default_columns])
            display_full(df)
        else:
            # display(df[default_columns].sort_values(by=['simtimeRaw', 'vectorName']))
            # display(df[default_columns])
            if args.query:
                display(df.query(args.query))
            else:
                display(df)


if __name__=='__main__':
    try:
        main()
    except Exception as e:
        print(f'Encountered error: {e}')
        print(''.join(traceback.format_exception(e)))
