import time
import pathlib
import re

from typing import Union, List, Callable

# ---

from yaml import YAMLObject

from sqlalchemy import create_engine

import pandas as pd
import seaborn as sb
import matplotlib as mpl

import dask
import dask.dataframe as ddf

import dask.distributed
from dask.delayed import Delayed

# ---

import sql_queries

from data_io import DataSet, read_from_file

from tag_extractor import ExtractRunParametersTagsOperation
from tag_regular_expressions import parameters_regex_map, attributes_regex_map, iterationvars_regex_map

from common.common_sets import BASE_TAGS_EXTRACTION, BASE_TAGS_EXTRACTION_MINIMAL

# ---

class FileResultProcessor(YAMLObject):
    def __init__(self, output_filename, *args, **kwargs):
        self.output_filename = output_filename

    def save_to_disk(self, df, filename, file_format='feather', compression='lz4', hdf_key='data'):
        start = time.time()

        if df is None:
            print('>>>> save_to_disk: input DataFrame is None')
            return

        if df.empty:
            print('>>>> save_to_disk: input DataFrame is empty')
            return

        if file_format == 'feather':
            df.reset_index().to_feather(filename
                                        , compression=compression
                                       )
        elif file_format == 'hdf':
            df.reset_index().to_hdf(filename
                                    , format='table'
                                    , key=hdf_key
                                   )
        else:
            raise Exception('Unknown file format')

        stop = time.time()
        print(f'{stop - start=}')
        print(f'>>>> save_to_disk: {self.dataset_name} saved to {self.output_filename}')
        print(f'>>>> save_to_disk: {df=}')

    def set_data_repo(self, data_repo):
        self.data_repo = data_repo

    def execute(self):
        # self.save_to_disk(self.data_repo[self.dataset_name], self.output_filename)

        data_list = self.data_repo[self.dataset_name]

        concat_result = dask.delayed(pd.concat)(data_list, ignore_index=True)
        convert_columns_result = dask.delayed(RawExtractor.convert_columns_to_category)(concat_result)

        job = dask.delayed(self.save_to_disk)(convert_columns_result, self.output_filename)
        # data = self.data_repo[self.dataset_name]
        # print(f'save_to_disk: {data.memory_usage(deep=True)=}')
        print(f'FileResultProcessor: execute: {job=}')
        return job


class Recipe(YAMLObject):
    pass
class Evaluation(YAMLObject):
    pass
class Plot(YAMLObject):
    pass

class Task(YAMLObject):
    def __init__(self):
        pass


class PlottingReaderFeather(YAMLObject):
    def __init__(self, input_files:str):
        self.input_files = input_files

    def read_data(self):
        data_set = DataSet(self.input_files)
        # data_list = []
        # for path in data_set.get_file_list():
        #     data = dask.delayed(read_from_file)(path)
        #     data_list.append(data)

        data_list = list(map(dask.delayed(read_from_file), data_set.get_file_list()))
        concat_result = dask.delayed(pd.concat)(data_list)
        convert_columns_result = dask.delayed(RawExtractor.convert_columns_to_category)(concat_result)
        print(f'{data_list=}')
        print(f'{convert_columns_result=}')
        # d = dask.compute(convert_columns_result)
        # print(f'{d=}')
        return [convert_columns_result]

        # exit(23)

        # data = pd.concat(data_list, ignore_index=True)
        # n = len(data)
        # # data = RawExtractor.convert_columns_to_category(data, [], n)
        # print(f'{data=}')
        # return data


class PlottingTask(YAMLObject):
    def __init__(self, data_repo:dict
                 , plot_type:str
                 , x:str, y:str
                 , columns:str, rows:str
                 , hue:str, style:str
                 ):
        print('BING')
        self.data_repo = data_repo
        self.plot_type = plot_type

    def set_data_repo(self, data_repo:dict):
        self.data_repo = data_repo

    def load_data(self):
        reader = PlottingReaderFeather(self.input_files)
        self.data = reader.read_data()

    def set_defaults(self):
        mpl.use(self.matplotlib_backend)

        sb.set_theme(style=self.axes_style)


    def plot_data(self, data):
        # print(f'{data=}')
        print(f'{data.memory_usage(deep=True)=}')

        if hasattr(self, 'selector'):
            selected_data = data.query(self.selector)
            # print(f'after selector: {data=}')
        # print(f'{selected_data=}')
        # print(f'{set(selected_data["prefix"])=}')

        # if not hasattr(self, 'hue'):
        #     self.hue = None
        # if not hasattr(self, 'style'):
        #     self.style = None
        # if not hasattr(self, 'row'):
        #     self.row = None
        # if not hasattr(self, 'column'):
        #     self.column = None

        for attr in [ 'hue', 'style', 'row', 'column' ]:
            if not hasattr(self, attr):
                setattr(self, attr, None)

        def catplot(plot_type):
                return self.plot_catplot(df=selected_data
                                        , plot_type=plot_type
                                        , x=self.x, y=self.y
                                        , hue=self.hue
                                        , row=self.row, column=self.column
                                       )

        def relplot(plot_type):
                return self.plot_relplot(df=selected_data
                                        , plot_type=plot_type
                                        , x=self.x, y=self.y
                                        , hue=self.hue, style=self.style
                                        , row=self.row, column=self.column
                                       )

        fig = None
        match self.plot_type:
            case 'lineplot':
                fig = relplot('line')
            case 'scatterplot':
                fig = relplot('scatter')
            case 'box':
                fig = catplot('box')
            case 'boxen':
                fig = catplot('boxen')
            case 'stripplot':
                fig = catplot('strip')
            case 'swarm':
                fig = catplot('swarm')
            case 'bar':
                fig = catplot('bar')
            case 'count':
                fig = catplot('count')
            case 'point':
                fig = catplot('point')
            case _:
                raise Exception(f'Unknown plot type: "{self.plot_type}"')

        fig.tight_layout(pad=0.1)

        fig.savefig(self.output_file, bbox_inches=self.bbox_inches)
        print(f'{fig=} saved to {self.output_file}')


    def execute(self):
        self.set_defaults()

        data = self.data_repo[self.dataset_name]
        print('----------------======-------------')
        print(f'{self.data_repo=}')
        print(f'{self.dataset_name=}')
        # print(f'{data=}')
        # print(f'><<<><<<<<>><<>>> execute: {data[0]=}')
        # print(f'><<<><<<<<>><<>>> execute: {data[0].compute()=}')
        cdata = dask.delayed(pd.concat)(data)
        # print(f'><<<><<<<<>><<>>> execute: {cdata.compute()=}')
        # print(f'><<<><<<<<>><<>>> execute: {cdata.compute().memory_usage(deep=True)=}')
        # exit(23)

        job = dask.delayed(self.plot_data)(cdata)

        return job


    def savefigure(self, fig, plot_destination_dir, filename, bbox_inches='tight'):
        """
        Save the given figure as PNG & SVG in the given directory with the given filename
        """
        def save_figure_with_type(extension):
            path = f'{plot_destination_dir}/{filename}.{extension}'
            fig.savefig(path, bbox_inches=bbox_inches)

        save_figure_with_type('png')
        #save_figure_with_type('svg')
        save_figure_with_type('pdf')

    def plot_catplot(self, df, x='v2x_rate', y='cbr', hue='moduleName', row='dcc', column='traciStart', plot_type='line'):
        # fig = plt.figure()
        # ax = fig.add_subplot()


        props_args = {
                'boxprops': {'edgecolor': 'black'}
                , 'medianprops': {'color':'red'}
                , 'flierprops': dict(color='red', marker='+', markersize=3, markeredgecolor='red', linewidth=0.1, alpha=0.1)
        }

        print('------    BING -------')
        # print(f'{df=}')
        print(f'{plot_type=}')

        if plot_type == 'box':
            kwargs = props_args
        else:
            kwargs = {}

        grid = sb.catplot(data=df, x=x, y=y, row=row, col=column
                        , hue=hue
                        # , hue_order=['itsg5_FFK_SCO', 'itsg5_FFK_MCO_MCM', 'itsg5_FFK_MCO_MCM-IDSM']
                        , kind=plot_type
                        # , boxprops=boxprops, medianprops=medianprops, flierprops=flierprops
                        # , legend_out=False
                        , **kwargs
                       )

        print('------    BING -------')

        grid = self.set_grid_defaults(grid)
        
        return grid


    def set_grid_defaults(self, grid):
        # ax.fig.gca().set_ylim(ylimit)
        for axis in grid.figure.axes:
            # axis.set_ylim((-30, 250))
            axis.set_xlabel(self.xlabel)
            axis.set_ylabel(self.ylabel)

        # strings of length of zero evaluate to false, so test explicitly for None
        if not self.title_template == None:
            grid.set_titles(template=self.title_template)
            print(f'{self.title_template=}')

        # print(type(ax))
        # ax.fig.get_axes()[0].legend(loc='lower left', bbox_to_anchor=(0, 1, 1, 1))

        if hasattr(self, 'legend_title'):
            sb.move_legend(grid, loc=self.legend_location, title=self.legend_title)
        else:
            sb.move_legend(grid, loc=self.legend_location)

        return grid


    def plot_relplot(self, df, x='v2x_rate', y='cbr', hue='moduleName', style='prefix', row='dcc', column='traciStart', plot_type='line', **kwargs):
        # fig = plt.figure()
        # ax = fig.add_subplot()

        boxprops = {'edgecolor': 'black'}
        medianprops = {'color':'red'}
        flierprops = dict(color='red', marker='+', markersize=3, markeredgecolor='red', linewidth=0.1, alpha=0.1)

        grid = sb.relplot(data=df, x=x, y=y, row=row, col=column
                        , hue=hue
                        # , hue_order=['itsg5_FFK_SCO', 'itsg5_FFK_MCO_MCM', 'itsg5_FFK_MCO_MCM-IDSM']
                        , kind=plot_type
                        , style=style
                        , alpha=self.alpha
                        # , boxprops=boxprops, medianprops=medianprops, flierprops=flierprops
                        # , legend_out=False
                        , **kwargs
                       )

        grid = self.set_grid_defaults(grid)

        return grid


class PlottingPreProcTask(YAMLObject):
    pass
class PlottingPostProcTask(YAMLObject):
    pass

class Extractor:
    def execute(self):
        return None


class SqlLiteReader():
    def __init__(self, db_file):
        self.db_file = db_file
        self.connect()

    def connect(self):
        self.engine = create_engine("sqlite:///"+self.db_file)
        self.connection = self.engine.connect()

    def disconnect(self):
        self.engine.disconnect()
        self.engine.close()

    def execute_sql_query(self, query):
        result = pd.read_sql_query(query, self.connection)
        return result

    def parameter_extractor(self):
        result = pd.read_sql_query(sql_queries.run_param_query, self.connection)
        return result

    def attribute_extractor(self):
        result = pd.read_sql_query(sql_queries.run_attr_query, self.connection)
        return result

    def extract_tags(self):
        tags = ExtractRunParametersTagsOperation.extract_attributes_and_params(self.parameter_extractor, self.attribute_extractor
                                                                               , parameters_regex_map, attributes_regex_map, iterationvars_regex_map)
        return tags


class RawExtractor(SqlLiteReader, YAMLObject):
    yaml_tag = u'!recipe.RawExtractor'

    def __init__(self, signals:list, /,  *args, **kwargs):
        self.signals:list = signals


    @staticmethod
    def apply_tags(data, tags, base_tags=None, additional_tags=[], minimal=True):
        if base_tags:
            allowed_tags = set(base_tags + additional_tags)
        else:
            if minimal:
                allowed_tags = set(BASE_TAGS_EXTRACTION_MINIMAL + additional_tags)
            else:
                allowed_tags = set(BASE_TAGS_EXTRACTION + additional_tags)

        # augment data with the extracted parameter tags
        for tag in tags:
            mapping = tag.get_mapping()
            if (not minimal) \
                    or (list(mapping)[0] in allowed_tags):
                data = data.assign(**mapping)

        return data

    @staticmethod
    def convert_columns_to_category(data, additional_columns:list = []):

        tag_names = list(parameters_regex_map.keys()) + list(iterationvars_regex_map.keys())
        categoricals_list = ['rowId', 'moduleName', 'traciStart', 'dcc',
                             'v2x_rate', 'configname', 'datetime', 'experiment', 'repetition',
                             'replication', 'runnumber', 'seedset', 'prefix']
        categoricals_list_ext = categoricals_list + tag_names + additional_columns
        # only check columns actually present in the input DataFrame
        categoricals_list_ext = list(set(categoricals_list_ext).intersection(set(data.columns)))

        col_list = list()
        threshold = len(data) / 2
        for col in categoricals_list_ext:
            # if not col in data.columns:
            #     continue
            # if the number of categories is larger than half the number of data
            # samples, don't convert the column
            s = len(set(data[col]))
            # print(f'{col}: {s}')
            if s < threshold:
                col_list.append(col)

        # print(f'{col_list=}')
        # convert selected columns to Categorical
        for col in col_list:
            data[col] = data[col].astype('category')
            data[col] = data[col].cat.as_ordered()

        return data

    @staticmethod
    def read_signals_from_file(db_file, signal, alias):
            sql_reader = SqlLiteReader(db_file)

            try:
                tags = sql_reader.extract_tags()
            except Exception:
                print(f'>>>> ERROR: no tags could be extracted from {db_file}')
                return pd.DataFrame()

            query = sql_queries.generate_signal_query(signal, value_label=alias)
            # print(f'{query=}')
            try:
                data = sql_reader.execute_sql_query(query)
            except Exception:
                print(f'>>>> ERROR: no data could be extracted from {db_file}')
                return pd.DataFrame()

            data = RawExtractor.apply_tags(data, tags)

            # select columns with a small enough set of possible values to
            # convert into `Categorical`
            data = RawExtractor.convert_columns_to_category(data)

            return data

    def prepare(self):
        data_set = DataSet(self.input_files)
        # print(f'{data_set.get_file_list()=}')

        # For every input file construct a `Delayed` object, a kind of a promise
        # on the data and the leafs of the computation graph
        result_list = []
        for db_file in data_set.get_file_list():
            # print(f'<-<-<-<-<--<====-=-=-=-=-==-=-=-=-=-=-<<-<-<-<--<<-<-<')
            # print(f'{db_file=}')
            # r = read_signals_from_file(db_file, self.signals)
            res = dask.delayed(RawExtractor.read_signals_from_file)(db_file, self.signal, self.alias)
            # q = dask.delayed(RawExtractor.convert_columns_to_category)(r)
            result_list.append(res)

        # print(f'{result_list=}')
        return result_list


class Transform(YAMLObject):
    yaml_tag = u'!recipe.Transform'

    def __init__(self, data_repo:dict):
        self.data_repo = data_repo

    def set_data_repo(self, data_repo:dict):
        self.data_repo = data_repo

    def execute(self):
        pass

class NullTransform(Transform, YAMLObject):
    yaml_tag = u'!recipe.NullTransform'

    def execute(self):
        pass

class FuntionTransform(Transform, YAMLObject):
    yaml_tag = u'!recipe.FunctionTransform'

    def execute(self):
        print(f'!recipe.FunctionTransform')
        data = self.data_repo[self.dataset_name]
        function = eval(self.function)

        data[self.output_column] = data[self.input_column].apply(function)
        self.data_repo[self.output_dataset_name] = data
        # print(f'{self.data_repo=}')


# class MeanTransform(Transform, YAMLObject):
#     yaml_tag = u'!recipe.MeanTransform'

#     def __init__(self, dataset_name:str, output_dataset_name:str
#                  , input_column:str, output_column:str):
#         self.dataset_name = dataset_name
#         self.output_dataset_name = output_dataset_name
#         self.input_column = input_column
#         self.output_column = output_column

#     def execute(self):
#         data = self.data_repo[self.dataset_name]
#         result = data[self.input_column].mean()
#         # print(f'{result=}')

#         # data[self.output_column] = result

#         row = data.head(n=1)
#         row = row.drop(labels=[self.input_column], axis=1)
#         row[self.output_column] = result

#         self.data_repo[self.output_dataset_name] = row

# class StatisticsTransform(Transform, YAMLObject):
#     yaml_tag = u'!recipe.StatisticsTransform'

#     def __init__(self, dataset_name:str, output_dataset_name:str
#                  , input_column:str, output_column:str):
#         self.dataset_name = dataset_name
#         self.output_dataset_name = output_dataset_name
#         self.input_column = input_column
#         self.output_column = output_column

#     def execute(self):
#         data = self.data_repo[self.dataset_name]
#         result = data[self.input_column].describe()
#         # print(f'{result=}')

#         # data[self.output_column] = result

#         row = data.head(n=1)
#         row = row.drop(labels=[self.input_column], axis=1)
#         # row[self.output_column] = result
#         print(pd.DataFrame(result).to_dict()[self.input_column])
#         row = row.assign(**pd.DataFrame(result).to_dict()[self.input_column])

#         self.data_repo[self.output_dataset_name] = row


class GroupedAggregationTransform(Transform, YAMLObject):
    yaml_tag = u'!recipe.GroupedAggregationTransform'

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
        # print(f'aggregate_frame: {data=}')
        if len(self.grouping_columns) == 1:
            grouping_columns = self.grouping_columns[0]
        else:
            grouping_columns = self.grouping_columns

        result_list = []
        for group_key, group_data in data.groupby(by=grouping_columns, sort=False):
            # print(f'{group_key=}')
            # print(f'{group_data=}')
            # result = group_data[self.input_column].mean()
            # result = dask.dataframe.groupby.DataFrameGroupBy.sum(group_data[self.input_column])
            # result = dask.dataframe.groupby.DataFrameGroupBy.sum(group_data[self.input_column])
            result = self.aggregation_function(group_data[self.input_column])

            row = group_data.head(n=1)
            row = row.drop(labels=[self.input_column], axis=1)
            row[self.output_column] = result

            result_list.append(row)
            # print(f'{row=}')
        # print('----->-----<<--------<-------<<<<---------')
        # print(f'-----> aggregate_frame: {len(result_list)=}')
        # print(f'-----> aggregate_frame: {result_list=}')
        result = pd.concat(result_list, ignore_index=True)
        # print(f'-----> aggregate_frame: {result=}')
        # exit(23)

        return result

    # def concat_and_categorize(df_list):
    #     data = pd.concat(df_list, ignore_index=True)
    #     data = RawExtractor.convert_columns_to_category(data)
    #     return data

    def execute(self):
        self.aggregation_function = eval(self.aggregation_function)
        data = self.data_repo[self.dataset_name]

        jobs = []
        for d in data:
            # print(f'execute: {d=}')
            job = dask.delayed(self.aggregate_frame)(d)
            jobs.append(job)

        # self.data_repo[self.output_dataset_name] = pd.concat(jobs, ignore_index=True)
        self.data_repo[self.output_dataset_name] = jobs
        # print('----->-----<<--------<-------<<<<---------')
        # print(f'-----> execute: {jobs=}')
        # print(f'-----> execute: {jobs[0].compute()=}')
        # exit(23)


class GroupedStatisticsTransform(Transform, YAMLObject):
    yaml_tag = u'!recipe.GroupedStatisticsTransform'

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
        # data = self.data_repo[self.dataset_name]

        result_list = []
        for group_key, group_data in data.groupby(by=self.grouping_columns, sort=False):
            result = group_data[self.input_column].describe()

            row = group_data.head(n=1)
            row = row.drop(labels=[self.input_column], axis=1)
            row = row.assign(**pd.DataFrame(result).to_dict()[self.input_column])

            result_list.append(row)

        # self.data_repo[self.output_dataset_name] = pd.concat(result_list, ignore_index=True)
        result = pd.concat(result_list, ignore_index=True)
        print(f'calculate_stats: {result=}')

        return result

    def execute(self):
        data = self.data_repo[self.dataset_name]

        jobs = []
        for d in data:
            print(f'execute: {d=}')
            job = dask.delayed(self.calculate_stats)(d)
            jobs.append(job)

        self.data_repo[self.output_dataset_name] = jobs

