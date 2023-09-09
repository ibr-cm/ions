import operator
from typing import Union, List, Callable, Optional

from collections import defaultdict

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

# for debugging purposes
from common.debug import start_ipython_dbg_cmdline


class Transform(YAMLObject):
    r"""
    The base class for all transforms
    """
    yaml_tag = u'!Transform'

    def set_data_repo(self, data_repo:dict):
        self.data_repo = data_repo

    def get_data(self, dataset_name:str):
        if not dataset_name in self.data_repo:
            raise Exception(f'"{dataset_name}" not found in data repo')

        data = self.data_repo[dataset_name]

        if data is None:
            raise Exception(f'data for "{dataset_name}" is None')

        return data

    def process(self, data:pd.DataFrame):
        # process data here
        return data

    def prepare(self):
        # The code below is just to illustrate the general procedure when
        # implementing a transform, it is not used

        # get the list of DataFrames in the dataset
        data_list = self.get_data(self.dataset_name)
        job_list = []

        for data in data_list:
            # construct a promise on the data produced by applying the function
            # to the input data
            function = lambda x: x
            job = dask.delayed(self.process)(data, function)
            job_list.append(job)

        # set the output dataset to the list of promises so that other tasks can
        # depend on and use them
        self.data_repo[self.output_dataset_name] = job_list

        return job_list


class NullTransform(Transform, YAMLObject):
    yaml_tag = u'!NullTransform'

    def prepare(self):
        pass


class ConcatTransform(Transform, YAMLObject):
    r"""
    A transform for concatenating datasets.

    Parameters
    ----------
    dataset_names: Optional[List[str]]
        the list of datasets to concatenate

    output_dataset_name: str
        the name given to the output dataset
    """

    yaml_tag = u'!ConcatTransform'

    def __init__(self, dataset_names:Optional[List[str]]
                 , output_dataset_name:str):
        self.dataset_names = dataset_names
        self.output_dataset_name = output_dataset_name

    def concat(self, dfs:List[pd.DataFrame]):
        r = pd.concat(dfs)
        return r

    def prepare(self):
        data_list = []
        for name in self.dataset_names:
            data_list.extend(self.get_data(name))

        # concatenate all DataFrames
        job = dask.delayed(self.concat)(map(operator.itemgetter(0), data_list))

        attributes = DataAttributes()

        # add all source files as attributes
        for attribute in list(map(operator.itemgetter(1), data_list)):
            for source_file in attribute.get_source_files():
                attributes.add_source_file(source_file)

        # allow other tasks to depend on the output of the delayed jobs
        self.data_repo[self.output_dataset_name] = [(job, attributes)]

        return [(job, attributes)]


class MergeTransform(Transform, YAMLObject):
    r"""
    A transform for merging two datasets

    Parameters
    ----------
    dataset_name_left: str
        the left dataset to operate on

    dataset_name_right: str
        the right dataset to operate on

    output_dataset_name: str
        the name given to the output dataset

    left_key_columns: str
        the name of the column the function should be applied to

    left_key_columns: str
        the name given to the output column containing the results of applying
        the function

    """

    yaml_tag = u'!MergeTransform'

    def __init__(self, dataset_name_left:str
                 , dataset_name_right:str
                 , output_dataset_name:str
                 , left_key_columns:Optional[List[str]] = None
                 , right_key_columns:Optional[List[str]] = None
                 , match_by_filename:bool = True
                 , matching_attribute:str = 'source_file'
                 ):
        self.dataset_name_left = dataset_name_left
        self.dataset_name_right = dataset_name_right
        self.output_dataset_name = output_dataset_name

        self.left_key_columns = left_key_columns
        self.right_key_columns = right_key_columns

        self.match_by_filename = match_by_filename
        self.matching_attribute = matching_attribute

    def merge(self, data_l:pd.DataFrame, data_r:pd.DataFrame
              , left_key_columns:Optional[List[str]] = None
              , right_key_columns:Optional[List[str]] = None):
        def is_empty(df):
            if not df is None:
                if df.empty:
                    return True
                return False
            return True

        if is_empty(data_l):
            logd(f'left input to merge is empty: {data_l=}')
            return None
        if is_empty(data_l):
            logd(f'right input to merge is empty: {data_r=}')
            return None

        df_merged = data_l.merge(data_r, left_on=left_key_columns, right_on=right_key_columns, suffixes=['', '_r'])
        # start_ipython_dbg_cmdline(locals())
        return df_merged

    def prepare_matched_by_filename(self):
        data_list_l = self.get_data(self.dataset_name_left)
        data_list_r = self.get_data(self.dataset_name_right)

        job_list = []

        d = defaultdict(list)

        def add_by_source_file(data_list):
            for data, attributes in data_list:
                d[getattr(attributes, self.matching_attribute)].append((data, attributes))

        add_by_source_file(data_list_l)
        add_by_source_file(data_list_r)

        for source_file in d:
            (data_l, attributes_l), (data_r, attributes_r) = d[source_file]
            job = dask.delayed(self.merge)(data_l, data_r, self.left_key_columns, self.right_key_columns)

            # add the source files of both datasets to the set of dataset source files
            attributes = DataAttributes()
            attributes.add_source_file(attributes_l.source_file)
            attributes.add_source_file(attributes_r.source_file)
            attributes.add_alias(attributes_l.alias)
            attributes.add_alias(attributes_r.alias)
            job_list.append((job, attributes))

            logd(f'{attributes=}')
            # start_ipython_dbg_cmdline(locals())

        # allow other tasks to depend on the output of the delayed jobs
        self.data_repo[self.output_dataset_name] = job_list

        return job_list

    def prepare_simple_sequential(self):
        data_list_l = self.get_data(self.dataset_name_left)
        data_list_r = self.get_data(self.dataset_name_right)

        job_list = []

        for (data_l, attributes_l), (data_r, attributes_r) in zip(data_list_l, data_list_r):
            job = dask.delayed(self.merge)(data_l, data_r, self.left_key_columns, self.right_key_columns)
            attributes = DataAttributes()
            attributes.add_source_file(attributes_l.source_file)
            attributes.add_source_file(attributes_r.source_file)
            logd(f'{attributes=}')
            job_list.append((job, attributes_l))
            # start_ipython_dbg_cmdline(locals())

        # allow other tasks to depend on the output of the delayed jobs
        self.data_repo[self.output_dataset_name] = job_list

        return job_list

    def prepare(self):
        if self.match_by_filename:
            return self.prepare_matched_by_filename()
        else:
            return self.prepare_simple_sequential()


class FunctionTransform(Transform, YAMLObject):
    r"""
    A transform for applying a function to every value in a column of a DataFrame

    Parameters
    ----------
    dataset_name: str
        the dataset to operate on

    output_dataset_name: str
        the name given to the output dataset

    function: Callable
        the unary function to apply to each DataFrame of the dataset
    """

    yaml_tag = u'!FunctionTransform'

    def __init__(self, dataset_name:str, output_dataset_name:str
                 , function:Callable=pd.Series.mean):
        self.dataset_name = dataset_name
        self.output_dataset_name = output_dataset_name

        self.function = function

    def process(self, data, function, attributes) -> pd.DataFrame:
        if data is None or (not data is None and data.empty):
            return pd.DataFrame()

        return function(data)

    def prepare(self):
        data_list = self.get_data(self.dataset_name)

        if isinstance(self.function, Callable):
            function = self.function
        else:
            function = eval(self.function)

        job_list = []

        for data, attributes in data_list:
            job = dask.delayed(self.process)(data, function, attributes)
            job_list.append((job, attributes))

        # allow other tasks to depend on the output of the delayed jobs
        self.data_repo[self.output_dataset_name] = job_list

        return job_list


class ColumnFunctionTransform(Transform, YAMLObject):
    r"""
    A transform for applying a function to every value in a column of a DataFrame

    Parameters
    ----------
    dataset_name: str
        the dataset to operate on

    output_dataset_name: str
        the name given to the output dataset

    input_column: str
        the name of the column the function should be applied to

    output_column: str
        the name given to the output column containing the results of applying
        the function

    function: Callable
        the unary function to apply to the values in the chosen column

    """

    yaml_tag = u'!ColumnFunctionTransform'

    def __init__(self, dataset_name:str, output_dataset_name:str
                 , input_column:str, output_column:str
                 , function:Callable=pd.Series.mean):
        self.dataset_name = dataset_name
        self.output_dataset_name = output_dataset_name

        self.input_column = input_column
        self.output_column = output_column

        self.function = function

    def process(self, data, function, attributes):
        data[self.output_column] = data[self.input_column].apply(function)
        return data

    def prepare(self):
        data_list = self.get_data(self.dataset_name)

        if isinstance(self.function, Callable):
            function = self.function
        else:
            function = eval(self.function)

        job_list = []

        for data, attributes in data_list:
            job = dask.delayed(self.process)(data, function, attributes)
            job_list.append((job, attributes))

        # allow other tasks to depend on the output of the delayed jobs
        self.data_repo[self.output_dataset_name] = job_list

        return job_list

class GroupedAggregationTransform(Transform, YAMLObject):
    r"""
    A transform for dividing a dataset into distinct partitions with
    `pandas.DataFrame.groupby
    <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.groupby.html#pandas.DataFrame.groupby>`__,
    each sharing the same value in the specified list of grouping/partitioning
    column names, and then applying a function to the values in a given column
    of a that partition, producing a aggregate scalar value.

    Parameters
    ----------
    dataset_name: str
        the dataset to operate on

    output_dataset_name: str
        the name given to the output dataset

    input_column: str
        the name of the column the function should be applied to

    output_column: str
        the name given to the output column containing the results of applying
        the function

    grouping_columns: List
        the set of columns used for partitioning the dataset

    raw: bool
        whether to append the raw output of `transform_function` to the result list

    pre_concatenate: bool
        concatenate all input DataFrames before processing

    extra_code: Optional[str]
        this allows specifying additional code, like a more complex transform function

    aggregation_function: Callable
        the unary function to apply to a each partition. Should expect an
        `pandas.Series` as argument and return a scalar.

    timestamp_selector: Callable
        the function to select the row in the partition data as template for the output in case of aggregation
    """
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

    def prepare(self):
        data = self.get_data(self.dataset_name)

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
    r"""
    A transform for dividing a dataset into distinct partitions with
    `pandas.DataFrame.groupby
    <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.groupby.html#pandas.DataFrame.groupby>`__,
    each sharing the same value in the specified list of grouping/partitioning
    column names, and then applying a function to that partition.

    Parameters
    ----------
    dataset_name: str
        the dataset to operate on

    output_dataset_name: str
        the name given to the output dataset

    input_column: str
        the name of the column the function should be applied to

    output_column: str
        the name given to the output column containing the results of applying
        the function

    grouping_columns: List
        the set of columns used for partitioning the dataset

    raw: bool
        whether to append the raw output of `transform_function` to the result list

    aggregate: bool
        whether the transform function returns a scalar or an object (like a `pandas.DataFrame`)

    pre_concatenate: bool
        concatenate all input DataFrames before processing

    extra_code: Optional[str]
        this allows specifying additional code, like a more complex transform function

    transform_function: Union[Callable[[pandas.DataFrame], pandas.DataFrame], Callable[[pandas.DataFrame], object]]
        the unary function to apply to a each partition. Should expect an
        `pandas.DataFrame` as argument and return a `pandas.DataFrame`.

    timestamp_selector: Callable
        the function to select the row in the partition data as template for the output in case of aggregation
    """
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
                # just append the keys for the subset and the transformed DataFrame
                result_list.append((group_key, result))
            else:
                if self.aggregate:
                    # take the first row of the data and use it as a template
                    # row for the output DataFrame
                    row = group_data.head(n=1)
                    row = row.drop(labels=[self.input_column], axis=1)
                    # add the results a new column
                    row[self.output_column] = result
                    # print(f'<<<<>>>>>    {row=}')
                    result_list.append(row)
                else:
                    # add the results a new column
                    group_data[self.output_column] = result
                    result_list.append(group_data)
                    # print(f'<<<<>>>>>    {group_data=}')

        if not self.raw:
            # concatenate all the partitions
            result = pd.concat(result_list, ignore_index=True)
        else:
            result = result_list

        logd(f'GroupedFunctionTransform: {result=}')
        return result

    def prepare(self):
        data = self.get_data(self.dataset_name)

        jobs = []

        if self.pre_concatenate:
            # concatenate all input DataFrames before processing
            concat_result = dask.delayed(pd.concat)(map(operator.itemgetter(0), data), ignore_index=True)
            job = dask.delayed(self.aggregate_frame)(concat_result)
            # TODO: better DataAttributes
            jobs.append((job, DataAttributes(source_file=self.input_column, alias=self.output_column)))
        else:
            for d, attributes in data:
                job = dask.delayed(self.aggregate_frame)(d)
                jobs.append((job, attributes))

        # allow other tasks to depend on the output of the delayed job
        self.data_repo[self.output_dataset_name] = jobs

        return jobs

def register_constructors():
    yaml.add_constructor(u'!ConcatTransform', proto_constructor(ConcatTransform))
    yaml.add_constructor(u'!FunctionTransform', proto_constructor(FunctionTransform))
    yaml.add_constructor(u'!ColumnFunctionTransform', proto_constructor(ColumnFunctionTransform))
    yaml.add_constructor(u'!GroupedAggregationTransform', proto_constructor(GroupedAggregationTransform))
    yaml.add_constructor(u'!GroupedFunctionTransform', proto_constructor(GroupedFunctionTransform))
    yaml.add_constructor(u'!MergeTransform', proto_constructor(MergeTransform))

