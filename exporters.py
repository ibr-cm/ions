from typing import Union

import pathlib
import time
import operator

import yaml
from yaml import YAMLObject

import pandas as pd

# Pandas serializing handlers
import jsonpickle
import jsonpickle.ext.pandas as jsonpickle_pandas

import dask

from yaml_helper import proto_constructor

import logging
from common.logging_facilities import logi, loge, logd, logw, get_logging_level

from extractors import BaseExtractor

from utility.filesystem import check_file_access_permissions, check_directory_access_permissions


class FileResultProcessor(YAMLObject):
    r"""
    Export the given dataset as
    [feather/arrow](https://arrow.apache.org/docs/python/feather.html) or JSON

    Parameters
    ----------

    output_filename: str
        the name of the output file

    output_directory: str
        the path of the output directory

    dataset_name: str
        the name of the dataset to export

    format: str
        the output file format, either `feather` or `json`

    concatenate: bool
        Whether to concatenate the input data before exporting it. If false,
        the name of the output files will be derived from the input file names
        and the aliases in the data.

    raw: bool
        whether to save the raw input or convert the columns of the input
        `pandas.DataFrame` to categories before saving
    """
    yaml_tag = u'!FileResultProcessor'

    def __init__(self, dataset_name:str
                 , output_filename = None
                 , output_directory = None
                 , format:str = 'feather'
                 , concatenate:bool = False
                 , raw:bool = False
                 , categorical_columns:set[str] = set()
                 , numerical_columns:Union[dict[str, str], set[str]] = set()
                 , *args, **kwargs):
        if (not output_filename) and concatenate:
            raise ValueError('When concatenating a dataset into a single file, the `output_filename` must be specified')
        if (not output_directory) and (not concatenate):
            raise ValueError('When not concatenating a dataset into a single file, the `output_directory` must be specified')

        if output_filename and concatenate:
            check_file_access_permissions(output_filename)
        if output_directory and (not concatenate):
            check_directory_access_permissions(output_directory)

        self.dataset_name = dataset_name
        self.output_filename = output_filename
        self.output_directory = output_directory
        self.format = format
        self.concatenate = concatenate
        self.raw = raw

        # categorical_columns and numerical_columns (if appropriate) are explicitly converted
        # to a set to alleviate the need for an explicit tag in the YAML recipe, since pyyaml
        # always interprets values in curly braces as dictionaries
        self.categorical_columns:set[str] = set(categorical_columns)
        if not isinstance(numerical_columns, dict):
            self.numerical_columns:set[str] = set(numerical_columns)
        else:
            self.numerical_columns:dict[str, str] = numerical_columns

    def save_to_disk(self, df, filename, file_format='feather', compression='lz4', hdf_key='data'):
        start = time.time()

        logi(f'Saving "{filename}" ...')
        if df is None:
            logw('>>>> save_to_disk: input DataFrame is None')
            return

        if not self.raw and df.empty:
            logw('>>>> save_to_disk: input DataFrame is empty')
            return

        check_file_access_permissions(filename)

        if file_format == 'feather':
            try:
                df.reset_index().to_feather(filename, compression=compression)
            except Exception as e:
                loge(f'An exception occurred while trying to save "{filename}":\n{e}')
                loge(f'df:\n{df}')
                return
        elif file_format == 'hdf':
            df.reset_index().to_hdf(filename
                                    , format='table'
                                    , key=hdf_key
                                   )
        elif file_format == 'json':
            try:
                f = open(filename, 'w')
                f.write(jsonpickle.encode(df, unpicklable=False, make_refs=False, keys=True))
                f.close()
            except Exception as e:
                loge(f'An exception occurred while trying to save "{filename}":\n{e}')
                loge(f'df:\n{df}')
                return
        else:
            raise Exception('Unknown file format')

        stop = time.time()
        logi(f'>>>> save_to_disk: it took {stop - start}s to save {filename}')
        if not self.raw:
            if (get_logging_level() == logging.DEBUG):
                logd(f'>>>> save_to_disk: {df.memory_usage(deep=True)=}')

    def set_data_repo(self, data_repo):
        self.data_repo = data_repo

    def get_data(self, dataset_name:str):
        if dataset_name not in self.data_repo:
            raise Exception(f'"{dataset_name}" not found in data repo')

        data = self.data_repo[dataset_name]

        if data is None:
            raise Exception(f'data for "{dataset_name}" is None')

        return data

    def prepare_concatenated(self, data_list, job_list):
        if self.raw:
            job = dask.delayed(self.save_to_disk)(tuple(map(operator.itemgetter(0), data_list)), self.output_filename, self.format)
        else:
            concat_result = dask.delayed(pd.concat)(tuple(map(operator.itemgetter(0), data_list)), ignore_index=True)
            if len(self.categorical_columns) != 0 or len(self.numerical_columns) != 0:
                convert_columns_result = dask.delayed(BaseExtractor.convert_columns_dtype)(concat_result, self.categorical_columns, self.numerical_columns)
                job = dask.delayed(self.save_to_disk)(convert_columns_result, self.output_filename, self.format)
            else:
                job = dask.delayed(self.save_to_disk)(concat_result, self.output_filename, self.format)

        job_list.append(job)

        return job_list

    def prepare_separated(self, data_list, job_list):
        for data, attributes in data_list:
            logd(f">>>>>>\n{attributes=}")
            if len(attributes.source_files) == 1:
                source_file_str = list(attributes.source_files)[0]
                source_file = str(pathlib.PurePath(source_file_str).stem)
            else:
                logd(">>>>>> Multiple source files")
                source_file = '_'.join(list(attributes.get_source_files()))

            if len(attributes.aliases) == 1:
                aliases = list(attributes.get_aliases())[0]
            else:
                aliases = '_'.join(list(attributes.get_aliases()))

            relative_file = list(attributes.source_files)[0].split(str(attributes.common_root))[1]
            output_filename = self.output_directory + '/' \
                              + relative_file \
                              + '_' \
                              + aliases \
                              + '.' + self.format

            logd(f'{output_filename=}')

            if len(self.categorical_columns) != 0 or len(self.numerical_columns) != 0:
                convert_columns_result = dask.delayed(BaseExtractor.convert_columns_dtype)(data, self.categorical_columns, self.numerical_columns)
                job = dask.delayed(self.save_to_disk)(convert_columns_result, output_filename, self.format)
            else:
                job = dask.delayed(self.save_to_disk)(data, output_filename, self.format)
            job_list.append(job)

        return job_list

    def prepare(self):
        data_list = self.get_data(self.dataset_name)

        job_list = []

        if self.concatenate:
            job_list = self.prepare_concatenated(data_list, job_list)
        else:
            job_list = self.prepare_separated(data_list, job_list)

        logd(f'FileResultProcessor: prepare: {job_list=}')
        return job_list

def register_jsonpickle_handlers():
    r"""
    Register the jsonpickle handlers for pickling pandas objects to JSON.
    """
    jsonpickle_pandas.register_handlers()

def register_constructors():
    r"""
    Register YAML constructors for all exporters
    """
    yaml.add_constructor(u'!FileResultProcessor', proto_constructor(FileResultProcessor))
