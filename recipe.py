import time
import pathlib
import re
import operator

from typing import Union, List, Callable

# ---

from common.logging_facilities import log, logi, loge, logd, logw

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
import tag_regular_expressions as tag_regex

from common.common_sets import BASE_TAGS_EXTRACTION_FULL, BASE_TAGS_EXTRACTION_MINIMAL \
                               , DEFAULT_CATEGORICALS_COLUMN_EXCLUSION_SET

# ---

class FileResultProcessor(YAMLObject):
    def __init__(self, output_filename, *args, **kwargs):
        self.output_filename = output_filename

    def save_to_disk(self, df, filename, file_format='feather', compression='lz4', hdf_key='data'):
        start = time.time()

        if df is None:
            logw('>>>> save_to_disk: input DataFrame is None')
            return

        if df.empty:
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
        else:
            raise Exception('Unknown file format')

        stop = time.time()
        logi(f'>>>> save_to_disk: it took {stop - start}s to save {filename}')
        # logd(f'>>>> save_to_disk: {df=}')
        logd(f'>>>> save_to_disk: {df.memory_usage(deep=True)=}')

    def set_data_repo(self, data_repo):
        self.data_repo = data_repo


    def execute_concatenated(self, data_list, job_list):
        concat_result = dask.delayed(pd.concat)(map(operator.itemgetter(0), data_list), ignore_index=True)
        convert_columns_result = dask.delayed(RawExtractor.convert_columns_to_category)(concat_result)
        job = dask.delayed(self.save_to_disk)(convert_columns_result, self.output_filename)
        job_list.append(job)

        return job_list

    def execute_separated(self, data_list, job_list):
        for data, attributes in data_list:
            convert_columns_result = dask.delayed(RawExtractor.convert_columns_to_category)(data)
            output_filename = str(pathlib.PurePath(self.output_filename).parent) + '/' \
                              + str(pathlib.PurePath(attributes.source_file).stem) \
                              + '_' +attributes.alias \
                              + '.feather'
            job = dask.delayed(self.save_to_disk)(convert_columns_result, output_filename)
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

        if hasattr(self, 'numerical_columns'):
            numerical_columns = self.numerical_columns
        else:
            numerical_columns = []

        data_list = list(map(dask.delayed(read_from_file), data_set.get_file_list()))
        concat_result = dask.delayed(pd.concat)(data_list)
        convert_columns_result = dask.delayed(RawExtractor.convert_columns_to_category)(concat_result, excluded_columns=numerical_columns)
        logd(f'PlottingReaderFeather::read_data: {data_list=}')
        logd(f'PlottingReaderFeather::read_data: {convert_columns_result=}')
        # d = dask.compute(convert_columns_result)
        # logd(f'{d=}')
        return [(convert_columns_result, DataAttributes())]


class PlottingTask(YAMLObject):
    def __init__(self, data_repo:dict
                 , plot_type:str
                 , x:str, y:str
                 , columns:str, rows:str
                 , hue:str, style:str
                 ):
        self.data_repo = data_repo
        self.plot_type = plot_type

    def set_data_repo(self, data_repo:dict):
        self.data_repo = data_repo

    def load_data(self):
        reader = PlottingReaderFeather(self.input_files)
        self.data = reader.read_data()

    def set_defaults(self):
        logi(f'set_defaults: using {self.matplotlib_backend=}')
        mpl.use(self.matplotlib_backend)

        sb.set_theme(style=self.axes_style)


    def plot_data(self, data):
        # logi(f'PlottingTask::plot_data: {data.memory_usage(deep=True)=}')
        self.set_defaults()


        if hasattr(self, 'selector'):
            selected_data = data.query(self.selector)
            # logi(f'after selector: {data=}')
        else:
            selected_data = data

        # TODO: the default shouldn't be defined here...
        if not hasattr(self, 'legend'):
            setattr(self, 'legend', True)

        if not hasattr(self, 'y_range'):
            setattr(self, 'y_range', None)
        else:
            y_range_tuple = tuple(list(map(lambda x: float(str.strip(x)), self.y_range.strip('()').split(','))))
            setattr(self, 'y_range', y_range_tuple)

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

        if not fig.legend is None and self.legend is None:
            fig.legend.remove()

        fig.savefig(self.output_file, bbox_inches=self.bbox_inches)
        logi(f'{fig=} saved to {self.output_file}')

        return fig


    def execute(self):
        # the defaults have to be set in the main thread
        self.set_defaults()

        data = self.data_repo[self.dataset_name]
        cdata = dask.delayed(pd.concat)(map(operator.itemgetter(0), data))
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


    def set_grid_defaults(self, grid):
        # ax.fig.gca().set_ylim(ylimit)
        for axis in grid.figure.axes:
            axis.set_xlabel(self.xlabel)
            axis.set_ylabel(self.ylabel)
            if self.y_range:
                axis.set_ylim(self.y_range)

        # strings of length of zero evaluate to false, so test explicitly for None
        if not self.title_template == None:
            grid.set_titles(template=self.title_template)

        # logi(type(ax))
        # ax.fig.get_axes()[0].legend(loc='lower left', bbox_to_anchor=(0, 1, 1, 1))

        if not grid.legend is None:
            if hasattr(self, 'legend_title'):
                sb.move_legend(grid, loc=self.legend_location, title=self.legend_title)
            else:
                sb.move_legend(grid, loc=self.legend_location)

        return grid


    def set_plot_specific_options(self, plot_type:str, kwargs:dict):
        boxprops = {'edgecolor': 'black'}
        medianprops = {'color':'red'}
        flierprops = dict(color='red', marker='+', markersize=3, markeredgecolor='red', linewidth=0.1, alpha=0.1)

        match plot_type:
            case 'line':
                kwargs['errorbar'] = 'sd'
            case 'box':
                kwargs['boxprops'] = boxprops
                kwargs['medianprops'] = medianprops
                kwargs['flierprops'] = flierprops

        return kwargs


    def plot_catplot(self, df, x='v2x_rate', y='cbr', hue='moduleName', row='dcc', column='traciStart', plot_type='box', **kwargs):
        kwargs = self.set_plot_specific_options(plot_type, kwargs)

        logd(f'PlottingTask::plot_catplot: {df.columns=}')
        grid = sb.catplot(data=df, x=x, y=y, row=row, col=column
                        , hue=hue
                        , kind=plot_type
                        # , legend_out=False
                        , **kwargs
                       )

        grid = self.set_grid_defaults(grid)

        return grid


    def plot_relplot(self, df, x='v2x_rate', y='cbr', hue='moduleName', style='prefix', row='dcc', column='traciStart', plot_type='line', **kwargs):
        kwargs = self.set_plot_specific_options(plot_type, kwargs)

        logd(f'PlottingTask::plot_relplot: {df.columns=}')
        grid = sb.relplot(data=df, x=x, y=y, row=row, col=column
                        , hue=hue
                        , kind=plot_type
                        , style=style
                        , alpha=self.alpha
                        # , legend_out=False
                        , **kwargs
                       )

        grid = self.set_grid_defaults(grid)

        return grid


class Extractor(YAMLObject):
    yaml_tag = u'!recipe.Extractor'

    def prepare(self):
        return None

    def set_tag_maps(self, attributes_regex_map, iterationvars_regex_map, parameters_regex_map):
        setattr(self, 'attributes_regex_map', attributes_regex_map)
        setattr(self, 'iterationvars_regex_map', iterationvars_regex_map)
        setattr(self, 'parameters_regex_map', parameters_regex_map)

class SqlLiteReader():
    def __init__(self, db_file):
        self.db_file = db_file
        self.connection = None
        self.engine = None

    def connect(self):
        self.engine = create_engine("sqlite:///"+self.db_file)
        self.connection = self.engine.connect()

    def disconnect(self):
        self.connection.close()

    def execute_sql_query(self, query):
        self.connect()
        result = pd.read_sql_query(query, self.connection)
        self.disconnect()
        return result

    def parameter_extractor(self):
        self.connect()
        result = pd.read_sql_query(sql_queries.run_param_query, self.connection)
        self.disconnect()
        return result

    def attribute_extractor(self):
        self.connect()
        result = pd.read_sql_query(sql_queries.run_attr_query, self.connection)
        self.disconnect()
        return result

    def extract_tags(self, attributes_regex_map, iterationvars_regex_map, parameters_regex_map):
        tags = ExtractRunParametersTagsOperation.extract_attributes_and_params(self.parameter_extractor, self.attribute_extractor
                                                                               , parameters_regex_map, attributes_regex_map, iterationvars_regex_map)
        return tags


class DataAttributes(YAMLObject):
    def __init__(self, source_file=None, alias=None):
        self.source_file = source_file
        self.alias = alias


class RawExtractor(Extractor):
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

        applied_tags = []
        # augment data with the extracted parameter tags
        for tag in tags:
            mapping = tag.get_mapping()
            if (not minimal) \
                    or (list(mapping)[0] in allowed_tags):
                data = data.assign(**mapping)
                applied_tags.append(tag)
        logd(f': {applied_tags=}')

        return data

    @staticmethod
    def convert_columns_to_category(data, additional_columns:list = [], excluded_columns:set = {}):
        excluded_columns = set(excluded_columns).union(DEFAULT_CATEGORICALS_COLUMN_EXCLUSION_SET)

        col_list = []
        threshold = len(data) / 4
        for col in data.columns:
            if col in excluded_columns:
                continue
            # if the number of categories is larger than half the number of data
            # samples, don't convert the column
            s = len(set(data[col]))
            if s < threshold:
                col_list.append(col)

        # convert selected columns to Categorical
        for col in col_list:
            data[col] = data[col].astype('category')
            data[col] = data[col].cat.as_ordered()

        return data

    @staticmethod
    def read_signals_from_file(db_file, signal, alias
                               , categorical_columns=[], excluded_categorical_columns=set()
                               , base_tags = None, additional_tags = []
                               , minimal_tags=True
                               , simtimeRaw=True
                               , moduleName=True
                               , eventNumber=True
                               , attributes_regex_map=tag_regex.attributes_regex_map
                               , iterationvars_regex_map=tag_regex.iterationvars_regex_map
                               , parameters_regex_map=tag_regex.parameters_regex_map
                               ):
            sql_reader = SqlLiteReader(db_file)

            try:
                tags = sql_reader.extract_tags(attributes_regex_map, iterationvars_regex_map, parameters_regex_map)
            except Exception as e:
                loge(f'>>>> ERROR: no tags could be extracted from {db_file}:\n {e}')
                return pd.DataFrame()

            query = sql_queries.generate_signal_query(signal, value_label=alias
                                                      , moduleName=moduleName, simtimeRaw=simtimeRaw, eventNumber=eventNumber)

            try:
                data = sql_reader.execute_sql_query(query)
            except Exception as e:
                loge(f'>>>> ERROR: no data could be extracted from {db_file}:\n {e}')
                return pd.DataFrame()

            if 'rowId' in data.columns:
                data = data.drop(labels=['rowId'], axis=1)

            data = RawExtractor.apply_tags(data, tags, base_tags=base_tags, additional_tags=additional_tags, minimal=minimal_tags)

            # don't categorize the column with the actual data
            excluded_categorical_columns = excluded_categorical_columns.union(set([alias]))

            # select columns with a small enough set of possible values to
            # convert into `Categorical`
            data = RawExtractor.convert_columns_to_category(data \
                                                            , additional_columns=categorical_columns \
                                                            , excluded_columns=excluded_categorical_columns
                                                            )

            return data

    def get_categorical_overrides(self):
        if hasattr(self, 'categorical_columns'):
            categorical_columns = self.categorical_columns
        else:
            categorical_columns = []

        if hasattr(self, 'categorical_columns_excluded'):
            categorical_columns_excluded = set(self.categorical_columns_excluded)
        else:
            categorical_columns_excluded = set()

        return categorical_columns, categorical_columns_excluded


    def get_tag_attributes(self):
        if hasattr(self, 'minimal_tags'):
            minimal = self.minimal_tags
        else:
            minimal = True
            setattr(self, 'minimal_tags', minimal)

        if hasattr(self, 'base_tags'):
            base_tags = self.base_tags
        else:
            if minimal:
                base_tags = BASE_TAGS_EXTRACTION_MINIMAL
            else:
                base_tags = BASE_TAGS_EXTRACTION_FULL
            setattr(self, 'base_tags', base_tags)

        if hasattr(self, 'additional_tags'):
            additional_tags = self.additional_tags
        else:
            additional_tags = []
            setattr(self, 'additional_tags', additional_tags)

        return minimal, base_tags, additional_tags


    def setup_output_columns(self):
        presets = [ ('simtimeRaw', True), ('moduleName', True), ('eventNumber', True) ]
        for k, v in presets:
            if not hasattr(self, k):
                setattr(self, k, v)


    def prepare(self):
        data_set = DataSet(self.input_files)

        self.setup_output_columns()

        categorical_columns, categorical_columns_excluded = self.get_categorical_overrides()
        minimal_tags, base_tags, additional_tags = self.get_tag_attributes()

        # For every input file construct a `Delayed` object, a kind of a promise
        # on the data and the leafs of the computation graph
        result_list = []
        for db_file in data_set.get_file_list():
            res = dask.delayed(RawExtractor.read_signals_from_file)\
                               (db_file, self.signal, self.alias \
                                , categorical_columns=categorical_columns \
                                , excluded_categorical_columns=categorical_columns_excluded \
                                , base_tags=base_tags, additional_tags=additional_tags
                                , minimal_tags=minimal_tags
                               , simtimeRaw=self.simtimeRaw
                               , moduleName=self.moduleName
                               , eventNumber=self.eventNumber
                               )
            attributes = DataAttributes(source_file=db_file, alias=self.alias)
            result_list.append((res, attributes))

        return result_list


class PositionExtractor(RawExtractor):
    yaml_tag = u'!recipe.PositionExtractor'


    @staticmethod
    def read_position_and_signal_from_file(db_file
                                           , x_signal:str
                                           , y_signal:str
                                           , x_alias:str
                                           , y_alias:str
                                           , signal:str
                                           , alias:str
                                           , restriction:tuple=None
                                           , moduleName:bool=True
                                           , simtimeRaw:bool=True
                                           , eventNumber:bool=False
                               , categorical_columns=[], excluded_categorical_columns=set()
                               , base_tags = None, additional_tags = []
                               , minimal_tags=True
                               , attributes_regex_map=tag_regex.attributes_regex_map
                               , iterationvars_regex_map=tag_regex.iterationvars_regex_map
                               , parameters_regex_map=tag_regex.parameters_regex_map
                               ):
            sql_reader = SqlLiteReader(db_file)

            try:
                tags = sql_reader.extract_tags(attributes_regex_map, iterationvars_regex_map, parameters_regex_map)
            except Exception as e:
                loge(f'>>>> ERROR: no tags could be extracted from {db_file}:\n {e}')
                return pd.DataFrame()

            query = sql_queries.get_signal_with_position(x_signal=x_signal, y_signal=y_signal
                                              , value_label_px=x_alias, value_label_py=y_alias
                                              , signal_name=signal, value_label=alias
                                              , restriction=restriction
                                              , moduleName=moduleName
                                              , simtimeRaw=simtimeRaw
                                              , eventNumber=eventNumber
                                              )

            try:
                data = sql_reader.execute_sql_query(query)
            except Exception as e:
                loge(f'>>>> ERROR: no data could be extracted from {db_file}:\n {e}')
                return pd.DataFrame()

            if 'rowId' in data.columns:
                data = data.drop(labels=['rowId'], axis=1)

            data = RawExtractor.apply_tags(data, tags, base_tags=base_tags, additional_tags=additional_tags, minimal=minimal_tags)

            # don't categorize the column with the actual data
            excluded_categorical_columns = excluded_categorical_columns.union(set([alias, x_alias, y_alias]))

            # select columns with a small enough set of possible values to
            # convert into `Categorical`
            data = RawExtractor.convert_columns_to_category(data \
                                                            , additional_columns=categorical_columns \
                                                            , excluded_columns=excluded_categorical_columns
                                                            )

            return data

    def prepare(self):
        data_set = DataSet(self.input_files)

        self.setup_output_columns()

        categorical_columns, categorical_columns_excluded = self.get_categorical_overrides()
        minimal_tags, base_tags, additional_tags = self.get_tag_attributes()

        if hasattr(self, 'restriction'):
            restriction = eval(self.restriction)
        else:
            restriction = None
            setattr(self, 'restriction', None)

        # For every input file construct a `Delayed` object, a kind of a promise
        # on the data and the leafs of the computation graph
        result_list = []
        for db_file in data_set.get_file_list():
            res = dask.delayed(PositionExtractor.read_position_and_signal_from_file)\
                               (db_file
                                , self.x_signal
                                , self.y_signal
                                , self.x_alias
                                , self.y_alias
                                , self.signal
                                , self.alias
                                , restriction=restriction
                                , moduleName=self.moduleName
                                , simtimeRaw=self.simtimeRaw
                                , eventNumber=self.eventNumber
                                , categorical_columns=categorical_columns \
                                , excluded_categorical_columns=categorical_columns_excluded \
                                , base_tags=base_tags, additional_tags=additional_tags
                                , minimal_tags=minimal_tags
                               )
            attributes = DataAttributes(source_file=db_file, alias=self.alias)
            result_list.append((res, attributes))

        return result_list


class MatchingExtractor(RawExtractor):
    yaml_tag = u'!recipe.MatchingExtractor'

    @staticmethod
    def get_matching_signals(db_file, pattern, alias_pattern):
        sql_reader = SqlLiteReader(db_file)
        # first, get the names of all the signals
        query = sql_queries.signal_names_query
        try:
            data = sql_reader.execute_sql_query(query)
        except Exception as e:
            loge(f'>>>> ERROR: no signal names could be extracted from {db_file}:\n {e}')
            return pd.DataFrame()

        # deduplicate the entries int the list of possible signals
        signals = list(set(data['vectorName']))

        # compile the signal matching regex
        regex = re.compile(pattern)

        # then check for matching signals
        matching_signals = []
        for signal in signals:
            r = regex.search(signal)
            if r:
                # construct the new name by substituting the matched and bound variables
                alias = alias_pattern.format(**r.groupdict())
                matching_signals.append((signal, alias))

        return matching_signals

    @staticmethod
    def extract_all_signals(db_file, signals
                            , categorical_columns=[], excluded_categorical_columns=set()
                            , base_tags=None, additional_tags=[]
                            , minimal_tags=True
                            , moduleName:bool=True
                            , simtimeRaw:bool=True
                            , eventNumber:bool=False
                            ):
        result_list = []
        for signal, alias in signals:
            res = RawExtractor.read_signals_from_file(db_file, signal, alias \
                                                      , categorical_columns=categorical_columns \
                                                      , excluded_categorical_columns=excluded_categorical_columns
                                                      , base_tags=base_tags, additional_tags=additional_tags
                                                      , minimal_tags=minimal_tags
                                                      , simtimeRaw=simtimeRaw
                                                      , moduleName=moduleName
                                                      , eventNumber=eventNumber
                                                     )
            result_list.append((res, alias))

        for i in range(0, len(result_list)):
            df = result_list[i][0]
            alias = result_list[i][1]
            # use all non-value column as primary (composite) key for the value column
            id_columns = list(set(df.columns).difference(set([alias])))
            # pivot the signal column into new rows
            df = df.melt(id_vars=id_columns, value_vars=alias, value_name='value')
            result_list[i] = df

        if len(result_list) > 0:
            result = pd.concat(result_list, ignore_index=True)
            result = RawExtractor.convert_columns_to_category(result
                                                                , additional_columns=categorical_columns \
                                                                , excluded_columns=excluded_categorical_columns
                                                             )
            return result
        else:
            return pd.DataFrame()

    def prepare(self):
        data_set = DataSet(self.input_files)

        self.setup_output_columns()

        categorical_columns, categorical_columns_excluded = self.get_categorical_overrides()
        minimal_tags, base_tags, additional_tags = self.get_tag_attributes()

        # For every input file construct a `Delayed` object, a kind of a promise
        # on the data, and the leafs of the task graph
        result_list = []
        for db_file in data_set.get_file_list():
            # get all signal names that match the given regular expression
            matching_signals_result = dask.delayed(MatchingExtractor.get_matching_signals)(db_file, self.pattern, self.alias_pattern)
            # get the data for the matched signals
            res = dask.delayed(MatchingExtractor.extract_all_signals)(db_file, matching_signals_result
                                                                       , categorical_columns, categorical_columns_excluded
                                                                       , base_tags=base_tags, additional_tags=additional_tags
                                                                       , minimal_tags=minimal_tags
                                                                       , simtimeRaw=self.simtimeRaw
                                                                       , moduleName=self.moduleName
                                                                       , eventNumber=self.eventNumber
                                                                       )
            attributes = DataAttributes(source_file=db_file, alias=self.alias)
            result_list.append((res, attributes))

        return result_list


class Transform(YAMLObject):
    yaml_tag = u'!recipe.Transform'

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
    yaml_tag = u'!recipe.NullTransform'

    def execute(self):
        pass


class FunctionTransform(Transform, YAMLObject):
    yaml_tag = u'!recipe.FunctionTransform'

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
