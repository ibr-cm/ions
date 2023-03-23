import operator
from typing import Union, List, Callable, Optional

import yaml
from yaml import YAMLObject

# some of the imports here are just here to make a base set of libraries
# available in the runtime environment of code fragments that are read and
# evaluated from a recipe
import numpy as np
import pandas as pd

import matplotlib
import matplotlib as mpl
import matplotlib.pyplot as plt

import seaborn as sb

import dask

from yaml_helper import decode_node, proto_constructor

from common.logging_facilities import logi, loge, logd, logw

from extractors import DataAttributes


class Transform(YAMLObject):
    yaml_tag = u'!Transform'

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

    def __init__(self, dataset_name:str, output_dataset_name:str
                 , input_column:str, output_column:str
                 , function:Callable=pd.Series.mean
                 , timestamp_selector:Callable=pd.DataFrame.head):
        self.dataset_name = dataset_name
        self.output_dataset_name = output_dataset_name

        self.input_column = input_column
        self.output_column = output_column

        self.function = function
        self.timestamp_selector = timestamp_selector


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
                 , raw:bool=False
                 , pre_concatenate:bool=False
                 , extra_code:Optional[str]=None
                 , aggregation_function:Callable=pd.Series.mean
                 , timestamp_selector:Callable=pd.DataFrame.head):
        self.dataset_name = dataset_name
        self.output_dataset_name = output_dataset_name

        self.input_column = input_column
        self.output_column = output_column

        self.grouping_columns = grouping_columns
        self.extra_code = extra_code
        self.aggregation_function = aggregation_function
        self.timestamp_selector = timestamp_selector

        self.raw = raw
        self.pre_concatenate = pre_concatenate

    def aggregate_frame(self, data):
        # create a copy of the global environment for evaluating the extra
        # code fragment so as to not pollute the global namespace itself
        global_env = globals().copy()
        locals_env = locals().copy()

        if type(self.extra_code) == str:
            # compile the code fragment
            self.extra_code = compile(self.extra_code, filename='<string>', mode='exec')
            # actually evaluate the code within the given namespace
            eval(self.extra_code, global_env, locals_env)

        if type(self.aggregation_function) == str:
            # evaluate the expression within the given environment
            self.aggregation_function = eval(self.aggregation_function, global_env, locals_env)

        if len(self.grouping_columns) == 1:
            grouping_columns = self.grouping_columns[0]
        else:
            grouping_columns = self.grouping_columns

        result_list = []
        for group_key, group_data in data.groupby(by=grouping_columns, sort=False):
            result = self.aggregation_function(group_data[self.input_column])

            if self.raw:
                result_list.append((group_key, result))
            else:
                row = group_data.head(n=1)
                row = row.drop(labels=[self.input_column], axis=1)
                row[self.output_column] = result
                result_list.append(row)

        if not self.raw:
            result = pd.concat(result_list, ignore_index=True)
        else:
            result = result_list

        return result

    def execute(self):
        data = self.data_repo[self.dataset_name]

        jobs = []

        if self.pre_concatenate:
            concat_result = dask.delayed(pd.concat)(map(operator.itemgetter(0), data), ignore_index=True)
            job = dask.delayed(self.aggregate_frame)(concat_result)
            # TODO: better DataAttributes
            jobs.append((job, DataAttributes(source_file=self.input_column, alias=self.output_column)))
        else:
            for d, attributes in data:
                job = dask.delayed(self.aggregate_frame)(d)
                jobs.append((job, attributes))

        self.data_repo[self.output_dataset_name] = jobs

        return jobs


class GroupedFunctionTransform(Transform, YAMLObject):
    yaml_tag = u'!GroupedFunctionTransform'

    def __init__(self, dataset_name:str, output_dataset_name:str
                 , input_column:str, output_column:str
                 , grouping_columns:List
                 , raw:bool=False
                 , aggregate:bool=False
                 , pre_concatenate:bool=False
                 , extra_code:Optional[str]=None
                 , transform_function:Callable=pd.DataFrame.head
                 , timestamp_selector:Callable=pd.DataFrame.head):
        self.dataset_name = dataset_name
        self.output_dataset_name = output_dataset_name

        self.input_column = input_column
        self.output_column = output_column

        self.grouping_columns = grouping_columns
        self.extra_code = extra_code
        self.transform_function = transform_function
        self.timestamp_selector = timestamp_selector

        self.raw = raw
        self.pre_concatenate = pre_concatenate
        self.aggregate = aggregate

        print(f'{type(self.raw)=}')
        print(f'{type(self.aggregate)=}')
        print(f'{type(self.pre_concatenate)=}')


    def aggregate_frame(self, data):
        if data.empty:
            return data
        # create a copy of the global environment for evaluating the extra
        # code fragment so as to not pollute the global namespace itself
        global_env = globals().copy()
        locals_env = locals().copy()

        logd(f'{data=}')
        # logd(f'{data.hour.unique()=}')

        if type(self.extra_code) == str:
            # compile the code fragment
            self.extra_code = compile(self.extra_code, filename='<string>', mode='exec')
            # actually evaluate the code within the given namespace
            eval(self.extra_code, global_env, locals_env)

        if type(self.transform_function) == str:
            # evaluate the expression within the given environment
            self.transform_function = eval(self.transform_function, global_env, locals_env)

        if len(self.grouping_columns) == 1:
            grouping_columns = self.grouping_columns[0]
        else:
            grouping_columns = self.grouping_columns

        result_list = []
        for group_key, group_data in data.groupby(by=grouping_columns, sort=False):
            result = self.transform_function(group_data)

            if self.raw:
                result_list.append((group_key, result))
            else:
                if self.aggregate:
                    row = group_data.head(n=1)
                    row = row.drop(labels=[self.input_column], axis=1)
                    row[self.output_column] = result
                    # print(f'<<<<>>>>>    {row=}')
                    result_list.append(row)
                else:
                    group_data[self.output_column] = result
                    result_list.append(group_data)
                    # print(f'<<<<>>>>>    {group_data=}')

        if not self.raw:
            result = pd.concat(result_list, ignore_index=True)
        else:
            result = result_list

        print(f'<<<<>>>>>    {result=}')
        return result

    def execute(self):
        data = self.data_repo[self.dataset_name]

        jobs = []

        if self.pre_concatenate:
            concat_result = dask.delayed(pd.concat)(map(operator.itemgetter(0), data), ignore_index=True)
            job = dask.delayed(self.aggregate_frame)(concat_result)
            # TODO: better DataAttributes
            jobs.append((job, DataAttributes(source_file=self.input_column, alias=self.output_column)))
        else:
            for d, attributes in data:
                job = dask.delayed(self.aggregate_frame)(d)
                jobs.append((job, attributes))

        self.data_repo[self.output_dataset_name] = jobs

        return jobs

def register_constructors():
    yaml.add_constructor(u'!GroupedAggregationTransform', proto_constructor(GroupedAggregationTransform))
    yaml.add_constructor(u'!GroupedFunctionTransform', proto_constructor(GroupedFunctionTransform))
    yaml.add_constructor(u'!FunctionTransform', proto_constructor(FunctionTransform))

