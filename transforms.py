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
from common.debug import start_ipython_dbg_cmdline, start_debug


class Transform(YAMLObject):
    r"""
    The base class for all transforms
    """
    yaml_tag = u'!Transform'

    def set_name(self, name:str):
        self.name = name

    def set_data_repo(self, data_repo:dict):
        r"""
        Parameters
        ----------
        data_repo : dict
            The dictionary containing all loaded datasets necessary for this transform
        """
        self.data_repo = data_repo

    def get_data(self, dataset_name:str):
        r"""
        Retrieve a dataset with the given name from the data repository associated with this transform

        Parameters
        ----------
        dataset_name : str
            The name of the dataset to retrieve from the data repository
        """
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


class ExtraCodeFunctionMixin:
    r"""
    A mixin class for providing the functionality to compile and evaluate a
    function and an additional, optional code fragment within a separate global environment.
    """
    def eval_function(self, function:Union[Callable, str], extra_code:Optional[str]) -> Callable:
        r"""
        Compile and evaluate the given function and an additional, optional
        code fragment within a separate global environment and return the
        executable function object.

        Parameters
        ----------
        function : Union[Callable, str]
            The name of the function or a function object.

        extra_code : Optional[str]
            This can contain additional code for the transform function, such as
            the definition of a function over multiple lines or split into multiple
            functions for readibility.
        """
        # create a copy of the global environment for evaluating the extra
        # code fragment so as to not pollute the global namespace itself
        global_env = globals().copy()

        if type(extra_code) == str:
            # compile the code fragment
            compiled_extra_code = compile(extra_code, filename='<string>', mode='exec')
            # actually evaluate the code within the given namespace to allow
            # access to all the defined symbols, such as helper functions that are not defined inline
            eval(compiled_extra_code, global_env)

        if isinstance(function, Callable):
            evaluated_function = function
        else:
            evaluated_function = eval(function, global_env)

        return evaluated_function


class ConcatTransform(Transform, YAMLObject):
    r"""
    A transform for concatenating all DataFrames from the given datasets.

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
        result = pd.concat(dfs)

        logd(f'ConcatTransform  "{self.name}" result:\n{result}')
        return result

    def prepare(self):
        data_list = []
        for name in self.dataset_names:
            data_list.extend(self.get_data(name))

        # concatenate all DataFrames
        job = dask.delayed(self.concat)(map(operator.itemgetter(0), data_list))

        attributes = DataAttributes()

        # add all source files as attributes
        for attribute in list(map(operator.itemgetter(1), data_list)):
            attributes.add_source_files(attribute.get_source_files())

        # allow other tasks to depend on the output of the delayed jobs
        self.data_repo[self.output_dataset_name] = [(job, attributes)]

        return [(job, attributes)]


class MergeTransform(Transform, YAMLObject):
    r"""
    A transform for merging the columns from two DataFrames, from two distinct
    datasets, similarly to a SQL INNER JOIN.

    Basically a wrapper around `pandas.merge <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.merge.html>`_

    Parameters
    ----------
    dataset_name_left: str
        the left dataset to operate on

    dataset_name_right: str
        the right dataset to operate on

    output_dataset_name: str
        the name given to the output dataset

    right_key_columns: str
        the name of the column from the right dataset taht is used as key for joining

    left_key_columns: str
        the name of the column from the left dataset taht is used as key for joining

    match_by_filename: bool
        whether to match merge input by the filename the data has been extracted from

    matching_attribute: str
        the attribute to match the datasets on

    """

    yaml_tag = u'!MergeTransform'

    def __init__(self, dataset_name_left:str
                 , dataset_name_right:str
                 , output_dataset_name:str
                 , left_key_columns:Optional[List[str]] = None
                 , right_key_columns:Optional[List[str]] = None
                 , match_by_filename:bool = True
                 , matching_attribute:str = 'source_files'
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

        logd(f'MergeTransform  "{self.name}" result:\n{df_merged}')
        return df_merged

    def prepare_matched_by_attribute(self):
        data_list_l = self.get_data(self.dataset_name_left)
        data_list_r = self.get_data(self.dataset_name_right)

        job_list = []

        d = defaultdict(list)

        def add_by_attribute(data_list):
            for data, attributes in data_list:
                attribute = getattr(attributes, self.matching_attribute)
                if type(attribute) == set:
                    attribute = '_'.join(list(attribute))
                d[attribute].append((data, attributes))

        add_by_attribute(data_list_l)
        add_by_attribute(data_list_r)

        for attribute in d:
            (data_l, attributes_l), (data_r, attributes_r) = d[attribute]
            job = dask.delayed(self.merge)(data_l, data_r, self.left_key_columns, self.right_key_columns)

            # add the source files of both datasets to the set of dataset source files
            attributes = DataAttributes()

            # add source files for both sources
            attributes.add_source_files(attributes_l.get_source_files())
            attributes.add_source_files(attributes_r.get_source_files())

            for alias in attributes_l.get_aliases():
                attributes.add_alias(alias)
            for alias in attributes_r.get_aliases():
                attributes.add_alias(alias)

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
            attributes.add_source_files(attributes_l.get_source_files())
            attributes.add_source_files(attributes_r.get_source_files())
            logd(f'{attributes=}')
            job_list.append((job, attributes))
            # start_ipython_dbg_cmdline(locals())

        # allow other tasks to depend on the output of the delayed jobs
        self.data_repo[self.output_dataset_name] = job_list

        return job_list

    def prepare(self):
        if self.match_by_filename:
            return self.prepare_matched_by_attribute()
        else:
            return self.prepare_simple_sequential()


class FunctionTransform(Transform, ExtraCodeFunctionMixin, YAMLObject):
    r"""
    A transform for applying a arbitrary function to a whole DataFrame.

    Parameters
    ----------
    dataset_name: str
        The dataset to operate on.

    output_dataset_name: str
        The name given to the output dataset.

    function: Union[Callable[[pandas.DataFrame], pandas.DataFrame], str]
        The unary function to apply to each DataFrame of the dataset.
        It takes the full DataFrame as its only argument and returns a DataFrame.

    extra_code: Optional[str]
        This can contain additional code for the transform function, such as
        the definition of a function over multiple lines or split into multiple
        functions for readibility.
    """

    yaml_tag = u'!FunctionTransform'

    def __init__(self, dataset_name:str, output_dataset_name:str
                 , function:Union[Callable[[pd.DataFrame], pd.DataFrame], str]=None
                 , extra_code:Optional[str]=None
                 ):
        self.dataset_name = dataset_name
        self.output_dataset_name = output_dataset_name

        if not function:
            msg = 'No processing function has been defined for FunctionTransform!'
            loge(msg)
            raise(TypeError(msg))

        self.function = function
        self.extra_code = extra_code

    def process(self, data, attributes) -> pd.DataFrame:
        if data is None or (not data is None and data.empty):
            return pd.DataFrame()

        # Get the function to call and possibly compile and evaluate the code defined in
        # extra_code in a separate global namespace.
        # The compilation of the extra code has to happen in the thread/process
        # of the processing worker since code objects can't be serialized.
        function = self.eval_function(self.function, self.extra_code)

        result = function(data)

        logd(f'FunctionTransform "{self.name}" result:\n{result}')
        return result

    def prepare(self):
        data_list = self.get_data(self.dataset_name)

        job_list = []

        for data, attributes in data_list:
            job = dask.delayed(self.process)(data, attributes)
            job_list.append((job, attributes))

        # allow other tasks to depend on the output of the delayed jobs
        self.data_repo[self.output_dataset_name] = job_list

        return job_list


class ColumnFunctionTransform(Transform, ExtraCodeFunctionMixin, YAMLObject):
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

    function: Union[Callable[[pandas.Series], pandas.Series], str]
        The unary function to apply to the values in the chosen column.

    extra_code: Optional[str]
        This can contain additional code for the transform function, such as
        the definition of a function over multiple lines or split into multiple
        functions for readibility.
    """

    yaml_tag = u'!ColumnFunctionTransform'

    def __init__(self, dataset_name:str, output_dataset_name:str
                 , input_column:str, output_column:str
                 , function:Union[Callable[[pd.Series], pd.Series], str]=None
                 , extra_code:Optional[str]=None
                 ):
        self.dataset_name = dataset_name
        self.output_dataset_name = output_dataset_name

        self.input_column = input_column
        self.output_column = output_column

        if not function:
            msg = f'No processing function has been defined for ColumnFunctionTransform!'
            loge(msg)
            raise(TypeError(msg))

        self.function = function
        self.extra_code = extra_code

    def process(self, data, attributes):
        # Get the function to call and possibly compile and evaluate the code defined in
        # extra_code in a separate global namespace.
        # The compilation of the extra code has to happen in the thread/process
        # of the processing worker since code objects can't be serialized.
        function = self.eval_function(self.function, None)

        data[self.output_column] = data[self.input_column].apply(function)

        logd(f'ColumnFunctionTransform  "{self.name}" result:\n{data}')
        return data

    def prepare(self):
        data_list = self.get_data(self.dataset_name)

        job_list = []

        for data, attributes in data_list:
            job = dask.delayed(self.process)(data, attributes)
            job_list.append((job, attributes))

        # allow other tasks to depend on the output of the delayed jobs
        self.data_repo[self.output_dataset_name] = job_list

        return job_list

class GroupedAggregationTransform(Transform, ExtraCodeFunctionMixin, YAMLObject):
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

    aggregation_function: Union[Callable[[pandas.Series], object], str]
        The unary function to apply to a each partition. Should expect an
        `pandas.Series` as argument and return a scalar value.

    extra_code: Optional[str]
        This can contain additional code for the transform function, such as
        the definition of a function over multiple lines or split into multiple
        functions for readibility.

    timestamp_selector: Callable
        the function to select the row in the partition data as template for the output in case of aggregation
    """
    yaml_tag = u'!GroupedAggregationTransform'

    def __init__(self, dataset_name:str, output_dataset_name:str
                 , input_column:str, output_column:str
                 , grouping_columns:List
                 , raw:bool=False
                 , pre_concatenate:bool=False
                 , aggregation_function:Union[Callable[[pd.Series], object], str]=None
                 , extra_code:Optional[str]=None
                 , timestamp_selector:Callable=pd.DataFrame.head):
        self.dataset_name = dataset_name
        self.output_dataset_name = output_dataset_name

        self.input_column = input_column
        self.output_column = output_column

        self.grouping_columns = grouping_columns

        if not aggregation_function:
            msg = f'No aggregation_function has been defined for GroupedAggregationTransform!'
            loge(msg)
            raise(TypeError(msg))

        self.aggregation_function = aggregation_function
        self.extra_code = extra_code

        self.timestamp_selector = timestamp_selector

        self.raw = raw
        self.pre_concatenate = pre_concatenate

    def aggregate_frame(self, data):
        if (data.empty):
            logw(f'GroupedAggregationTransform return is empty!')
            return pd.DataFrame()

        # Get the function to call and possibly compile and evaluate the code defined in
        # extra_code in a separate global namespace.
        # The compilation of the extra code has to happen in the thread/process
        # of the processing worker since code objects can't be serialized.
        aggregation_function = self.eval_function(self.aggregation_function, self.extra_code)

        if len(self.grouping_columns) == 1:
            grouping_columns = self.grouping_columns[0]
        else:
            grouping_columns = self.grouping_columns

        result_list = []
        for group_key, group_data in data.groupby(by=grouping_columns, sort=False, observed=True):
            result = aggregation_function(group_data[self.input_column])

            if self.raw:
                result_list.append((group_key, result))
            else:
                row = group_data.head(n=1)
                row = row.drop(labels=[self.input_column], axis=1)
                row[self.output_column] = result
                result_list.append(row)

        if result_list:
            if not self.raw:
                result = pd.concat(result_list, ignore_index=True)
            else:
                result = result_list
        else:
            logw(f'GroupedAggregationTransform result_list was empty!')
            return result_list

        logd(f'GroupedAggregationTransform "{self.name}" result:\n{result}')
        return result

    def prepare(self):
        data = self.get_data(self.dataset_name)

        jobs = []

        if self.pre_concatenate:
            concat_result = dask.delayed(pd.concat)(map(operator.itemgetter(0), data), ignore_index=True)
            job = dask.delayed(self.aggregate_frame)(concat_result)
            # TODO: better DataAttributes
            attributes = data[0][1]
            jobs.append((job, DataAttributes(source_file=self.input_column, alias=self.output_column, common_root=attributes.common_root)))
        else:
            for d, attributes in data:
                job = dask.delayed(self.aggregate_frame)(d)
                jobs.append((job, attributes))

        self.data_repo[self.output_dataset_name] = jobs

        return jobs


class GroupedFunctionTransform(Transform, ExtraCodeFunctionMixin, YAMLObject):
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

    transform_function: Union[Callable[[pandas.DataFrame], pandas.DataFrame], Callable[[pandas.DataFrame], object], str]
        The unary function to apply to a each partition. Should expect an
        `pandas.DataFrame` as argument and return a `pandas.DataFrame` (or an arbitrary object if `raw` is true).

    extra_code: Optional[str]
        This can contain additional code for the transform function, such as
        the definition of a function over multiple lines or split into multiple
        functions for readibility.

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
                 , transform_function:Union[Callable[[pd.DataFrame], pd.DataFrame], Callable[[pd.DataFrame], object], str]=None
                 , extra_code:Optional[str]=None
                 , timestamp_selector:Callable=pd.DataFrame.head):
        self.dataset_name = dataset_name
        self.output_dataset_name = output_dataset_name

        self.input_column = input_column
        self.output_column = output_column

        self.grouping_columns = grouping_columns

        if not transform_function:
            msg = f'No transform_function has been defined for GroupedFunctionTransform!'
            loge(msg)
            raise(TypeError(msg))

        self.transform_function = transform_function
        self.extra_code = extra_code

        self.timestamp_selector = timestamp_selector

        self.raw = raw
        self.pre_concatenate = pre_concatenate
        self.aggregate = aggregate

    def aggregate_frame(self, data):
        if data.empty:
            logw(f'GroupedFunctionTransform return is empty!')
            return pd.DataFrame()

        logd(f'{data=}')
        # logd(f'{data.hour.unique()=}')

        # Get the function to call and possibly compile and evaluate the code defined in
        # extra_code in a separate global namespace.
        # The compilation of the extra code has to happen in the thread/process
        # of the processing worker since code objects can't be serialized.
        transform_function = self.eval_function(self.transform_function, self.extra_code)

        if len(self.grouping_columns) == 1:
            grouping_columns = self.grouping_columns[0]
        else:
            grouping_columns = self.grouping_columns

        result_list = []
        for group_key, group_data in data.groupby(by=grouping_columns, sort=False, observed=True):
            result = transform_function(group_data)

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

        logd(f'GroupedFunctionTransform "{self.name}" result:\n{result}')
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
    r"""
    Register YAML constructors for all transforms
    """
    yaml.add_constructor(u'!ConcatTransform', proto_constructor(ConcatTransform))
    yaml.add_constructor(u'!FunctionTransform', proto_constructor(FunctionTransform))
    yaml.add_constructor(u'!ColumnFunctionTransform', proto_constructor(ColumnFunctionTransform))
    yaml.add_constructor(u'!GroupedAggregationTransform', proto_constructor(GroupedAggregationTransform))
    yaml.add_constructor(u'!GroupedFunctionTransform', proto_constructor(GroupedFunctionTransform))
    yaml.add_constructor(u'!MergeTransform', proto_constructor(MergeTransform))

