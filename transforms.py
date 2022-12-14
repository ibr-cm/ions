from typing import Union, List, Callable

from yaml import YAMLObject

import numpy as np
import pandas as pd

import dask

from common.logging_facilities import logi, loge, logd, logw


class Transform(YAMLObject):
    yaml_tag = u'!Transform'

    def __init__(self, data_repo:dict):
        self.data_repo = data_repo

    def set_data_repo(self, data_repo:dict):
        self.data_repo = data_repo

    def process(self, data:pd.DataFrame):
        # process data here
        return data

    def execute(self):
        # get the list of DataFrames in the dataset
        data_list = self.data_repo[self.dataset_name]
        job_list = []

        for data in data_list:
            # construct a promise on the data produced by applying the function
            # to the input data
            job = dask.delayed(self.process)(data, function)
            job_list.append(job)

        # set the output dataset to the list of promises so that other tasks can
        # depend on and use theme
        self.data_repo[self.output_dataset_name] = job_list

        return job_list


class NullTransform(Transform, YAMLObject):
    yaml_tag = u'!NullTransform'

    def execute(self):
        pass


class FunctionTransform(Transform, YAMLObject):
    yaml_tag = u'!FunctionTransform'

    def process(self, data, function, attributes):
        data[self.output_column] = data[self.input_column].apply(function)
        return data

    def execute(self):
        data_list = self.data_repo[self.dataset_name]
        if isinstance(self.function, Callable):
            function = self.function
        else:
            function = eval(self.function)

        job_list = []

        for data, attributes in data_list:
            job = dask.delayed(self.process)(data, function, attributes)
            job_list.append((job, attributes))

        self.data_repo[self.output_dataset_name] = job_list

        return job_list

class GroupedAggregationTransform(Transform, YAMLObject):
    yaml_tag = u'!GroupedAggregationTransform'

    def __init__(self, dataset_name:str, output_dataset_name:str
                 , input_column:str, output_column:str
                 , grouping_columns:List
                 , timestamp_selector:Callable=pd.DataFrame.head):
        self.dataset_name = dataset_name
        self.output_dataset_name = output_dataset_name
        self.input_column = input_column
        self.output_column = output_column
        self.grouping_columns = grouping_columns

    def aggregate_frame(self, data):
        # logi(f'aggregate_frame: {data=}')
        if len(self.grouping_columns) == 1:
            grouping_columns = self.grouping_columns[0]
        else:
            grouping_columns = self.grouping_columns

        result_list = []
        for group_key, group_data in data.groupby(by=grouping_columns, sort=False):
            # logi(f'{group_key=}')
            # logi(f'{group_data=}')
            # result = group_data[self.input_column].mean()
            result = self.aggregation_function(group_data[self.input_column])

            row = group_data.head(n=1)
            row = row.drop(labels=[self.input_column], axis=1)
            row[self.output_column] = result

            result_list.append(row)
            # logi(f'{row=}')
        # logi('----->-----<<--------<-------<<<<---------')
        # logi(f'-----> aggregate_frame: {len(result_list)=}')
        # logi(f'-----> aggregate_frame: {result_list=}')
        result = pd.concat(result_list, ignore_index=True)
        # logi(f'-----> aggregate_frame: {result=}')
        # exit(23)

        return result

    def execute(self):
        self.aggregation_function = eval(self.aggregation_function)
        data = self.data_repo[self.dataset_name]

        jobs = []
        for d, attributes in data:
            # logd(f'execute: {d=}')
            job = dask.delayed(self.aggregate_frame)(d)
            jobs.append((job, attributes))

        self.data_repo[self.output_dataset_name] = jobs

        return jobs


class GroupedStatisticsTransform(Transform, YAMLObject):
    yaml_tag = u'!GroupedStatisticsTransform'

    def __init__(self, dataset_name:str, output_dataset_name:str
                 , input_column:str, output_column:str
                 , grouping_columns:List
                 , timestamp_selector:Callable=pd.DataFrame.head):
        self.dataset_name = dataset_name
        self.output_dataset_name = output_dataset_name
        self.input_column = input_column
        self.output_column = output_column
        self.grouping_columns = grouping_columns

    def calculate_stats(self, data):
        result_list = []
        for group_key, group_data in data.groupby(by=self.grouping_columns, sort=False):
            result = group_data[self.input_column].describe()

            row = group_data.head(n=1)
            row = row.drop(labels=[self.input_column], axis=1)
            row = row.assign(**pd.DataFrame(result).to_dict()[self.input_column])

            result_list.append(row)

        result = pd.concat(result_list, ignore_index=True)
        logd(f'calculate_stats: {result=}')

        return result

    def execute(self):
        data = self.data_repo[self.dataset_name]

        jobs = []
        for d, attributes in data:
            logd(f'execute: {d=}')
            job = dask.delayed(self.calculate_stats)(d)
            jobs.append((job, attributes))

        self.data_repo[self.output_dataset_name] = jobs

        return jobs
