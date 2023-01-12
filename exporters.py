import pathlib
import time
import operator

import json

from yaml import YAMLObject

import numpy as np
import pandas as pd

import dask

from common.logging_facilities import logi, loge, logd, logw

from extractors import RawExtractor

class FileResultProcessor(YAMLObject):
    yaml_tag = u'!FileResultProcessor'

    def __init__(self, output_filename, *args, **kwargs):
        self.output_filename = output_filename

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
            f.write(json.dumps(df))
            f.close()

        else:
            raise Exception('Unknown file format')

        stop = time.time()
        logi(f'>>>> save_to_disk: it took {stop - start}s to save {filename}')
        if not self.raw:
            logd(f'>>>> save_to_disk: {df.memory_usage(deep=True)=}')

    def set_data_repo(self, data_repo):
        self.data_repo = data_repo


    def execute_concatenated(self, data_list, job_list):
        if self.raw:
            job = dask.delayed(self.save_to_disk)(map(operator.itemgetter(0), data_list), self.output_filename, self.format)
        else:
            concat_result = dask.delayed(pd.concat)(map(operator.itemgetter(0), data_list), ignore_index=True)
            convert_columns_result = dask.delayed(RawExtractor.convert_columns_to_category)(concat_result)
            job = dask.delayed(self.save_to_disk)(convert_columns_result, self.output_filename, self.format)

        job_list.append(job)

        return job_list

    def execute_separated(self, data_list, job_list):
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

    def execute(self):
        data_list = self.data_repo[self.dataset_name]

        job_list = []

        if self.concatenate:
            job_list = self.execute_concatenated(data_list, job_list)
        else:
            job_list = self.execute_separated(data_list, job_list)

        logd(f'FileResultProcessor: execute: {job_list=}')
        return job_list

