import pathlib
import time
import operator

import json

import yaml
from yaml import YAMLObject

import numpy as np
import pandas as pd

# Pandas serializing handlers
import jsonpickle
import jsonpickle.ext.pandas as jsonpickle_pandas
jsonpickle_pandas.register_handlers()

import dask

from yaml_helper import decode_node, proto_constructor

from common.logging_facilities import logi, loge, logd, logw

from extractors import RawExtractor

class FileResultProcessor(YAMLObject):
    r"""
    Export the given dataset as
    [feather/arrow](https://arrow.apache.org/docs/python/feather.html) or JSON

    Parameters
    ----------

    output_filename: str
        the name of the output file

    dataset_name: str
        the name of the dataset to export

    format: str
        the output file format, either `feather` or `json`

    concatenate: bool
        whether to concatenate the input data before exporting

    raw: bool
        whether to save the raw input or convert the columns of the input
        `pandas.DataFrame` to categories before saving
    """
    yaml_tag = u'!FileResultProcessor'

    def __init__(self, output_filename
                 , dataset_name:str
                 , format:str = 'feather'
                 , concatenate:bool = False
                 , raw:bool = False
                 , *args, **kwargs):
        self.output_filename = output_filename
        self.dataset_name = dataset_name
        self.format = format
        self.concatenate = concatenate
        self.raw = raw

    def save_to_disk(self, df, filename, file_format='feather', compression='lz4', hdf_key='data'):
        start = time.time()

        if df is None:
            logw('>>>> save_to_disk: input DataFrame is None')
            return

        if not self.raw and df.empty:
            logw('>>>> save_to_disk: input DataFrame is empty')
            return

        target_dir = pathlib.Path(pathlib.PurePath(filename).parent)
        if not target_dir.exists():
            target_dir.mkdir()

        if file_format == 'feather':
            df.reset_index().to_feather(filename
                                        , compression=compression
                                       )
        elif file_format == 'hdf':
            df.reset_index().to_hdf(filename
                                    , format='table'
                                    , key=hdf_key
                                   )
        elif file_format == 'json':
            f = open(filename, 'w')
            f.write(jsonpickle.encode(df, unpicklable=False, make_refs=False, keys=True))
            f.close()

        else:
            raise Exception('Unknown file format')

        stop = time.time()
        logi(f'>>>> save_to_disk: it took {stop - start}s to save {filename}')
        if not self.raw:
            logd(f'>>>> save_to_disk: {df.memory_usage(deep=True)=}')

    def set_data_repo(self, data_repo):
        self.data_repo = data_repo


    def prepare_concatenated(self, data_list, job_list):
        if self.raw:
            job = dask.delayed(self.save_to_disk)(map(operator.itemgetter(0), data_list), self.output_filename, self.format)
        else:
            concat_result = dask.delayed(pd.concat)(map(operator.itemgetter(0), data_list), ignore_index=True)
            convert_columns_result = dask.delayed(RawExtractor.convert_columns_to_category)(concat_result)
            job = dask.delayed(self.save_to_disk)(convert_columns_result, self.output_filename, self.format)

        job_list.append(job)

        return job_list

    def prepare_separated(self, data_list, job_list):
        for data, attributes in data_list:
            output_filename = str(pathlib.PurePath(self.output_filename).parent) + '/' \
                              + str(pathlib.PurePath(attributes.source_file).stem) \
                              + '_' +attributes.alias \
                              + '.' + self.format
            if self.raw:
                job = dask.delayed(self.save_to_disk)(data, output_filename, self.format)
            else:
                convert_columns_result = dask.delayed(RawExtractor.convert_columns_to_category)(data)
                job = dask.delayed(self.save_to_disk)(convert_columns_result, output_filename, self.format)

            job_list.append(job)

        return job_list

    def prepare(self):
        data_list = self.data_repo[self.dataset_name]

        job_list = []

        if self.concatenate:
            job_list = self.prepare_concatenated(data_list, job_list)
        else:
            job_list = self.prepare_separated(data_list, job_list)

        logd(f'FileResultProcessor: prepare: {job_list=}')
        return job_list


def register_constructors():
    yaml.add_constructor(u'!FileResultProcessor', proto_constructor(FileResultProcessor))

