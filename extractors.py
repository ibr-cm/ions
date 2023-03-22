
from typing import Optional, List, Set, Tuple

import re

# ---

from common.logging_facilities import logi, loge, logd, logw

# ---

import yaml
from yaml import YAMLObject

# ---

import numpy as np
import pandas as pd

# ---

import dask
import dask.dataframe as ddf

import dask.distributed
from dask.delayed import Delayed

# ---

from sqlalchemy import create_engine

# ---

import sql_queries

from yaml_helper import decode_node, proto_constructor

from data_io import DataSet, read_from_file

from tag_extractor import ExtractRunParametersTagsOperation
import tag_regular_expressions as tag_regex

from common.common_sets import BASE_TAGS_EXTRACTION_FULL, BASE_TAGS_EXTRACTION_MINIMAL \
                               , DEFAULT_CATEGORICALS_COLUMN_EXCLUSION_SET

# ---

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
    def __init__(self, /,  **kwargs):
        for key in kwargs:
            setattr(self, key, kwargs[key])

# ----------------------------------------


class Extractor(YAMLObject):
    yaml_tag = u'!Extractor'

    def prepare(self):
        return None

    def set_tag_maps(self, attributes_regex_map, iterationvars_regex_map, parameters_regex_map):
        setattr(self, 'attributes_regex_map', attributes_regex_map)
        setattr(self, 'iterationvars_regex_map', iterationvars_regex_map)
        setattr(self, 'parameters_regex_map', parameters_regex_map)


class BaseExtractor(Extractor):
    yaml_tag = u'!BaseExtractor'

    def __init__(self, /,
                 input_files:list
                 , categorical_columns:List[str] = []
                 , categorical_columns_excluded:List[str] = []
                 , base_tags:Optional[List] = None
                 , additional_tags:list = []
                 , minimal_tags:bool = True
                 , simtimeRaw:bool = True
                 , moduleName:bool = True
                 , eventNumber:bool = True
                 , *args, **kwargs
                 ):
        self.input_files:list = input_files

        self.categorical_columns:List[str] = categorical_columns
        self.categorical_columns_excluded:Set[str] = set(categorical_columns_excluded)

        if base_tags != None:
            self.base_tags:list = base_tags
        else:
            if minimal_tags:
                self.base_tags = BASE_TAGS_EXTRACTION_MINIMAL
            else:
                self.base_tags = BASE_TAGS_EXTRACTION_FULL

        self.additional_tags:list = additional_tags
        self.minimal_tags:bool = minimal_tags

        self.simtimeRaw:bool = simtimeRaw
        self.moduleName:bool = moduleName
        self.eventNumber:bool = eventNumber


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
            if list(mapping)[0] in allowed_tags:
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
    def read_statistic_from_file(db_file, scalar, alias
                               , runId:bool=True
                               , moduleName:bool=True
                               , statName:bool=False
                               , statId:bool=False
                               , **kwargs):
        query = sql_queries.generate_statistic_query(scalar
                                                  , runId=runId
                                                  , moduleName=moduleName
                                                  )

        return BaseExtractor.read_query_from_file(db_file, query, alias, **kwargs)


    @staticmethod
    def read_scalars_from_file(db_file, scalar, alias
                               , runId:bool=True
                               , moduleName:bool=True
                               , scalarName:bool=False
                               , scalarId:bool=False
                               , **kwargs):
        query = sql_queries.generate_scalar_query(scalar, value_label=alias
                                                  , runId=runId
                                                  , moduleName=moduleName
                                                  , scalarName=scalarName, scalarId=scalarId)

        return BaseExtractor.read_query_from_file(db_file, query, alias, **kwargs)


    @staticmethod
    def read_signals_from_file(db_file, signal, alias
                               , simtimeRaw=True
                               , moduleName=True
                               , eventNumber=True
                               , **kwargs):
        query = sql_queries.generate_signal_query(signal, value_label=alias
                                                  , moduleName=moduleName
                                                  , simtimeRaw=simtimeRaw
                                                  , eventNumber=eventNumber)

        return BaseExtractor.read_query_from_file(db_file, query, alias, **kwargs)


    @staticmethod
    def read_query_from_file(db_file, query, alias
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

            try:
                data = sql_reader.execute_sql_query(query)
            except Exception as e:
                loge(f'>>>> ERROR: no data could be extracted from {db_file}:\n {e}')
                return pd.DataFrame()

            if 'rowId' in data.columns:
                data = data.drop(labels=['rowId'], axis=1)

            data = BaseExtractor.apply_tags(data, tags, base_tags=base_tags, additional_tags=additional_tags, minimal=minimal_tags)

            # don't categorize the column with the actual data
            excluded_categorical_columns = excluded_categorical_columns.union(set([alias]))

            # select columns with a small enough set of possible values to
            # convert into `Categorical`
            data = BaseExtractor.convert_columns_to_category(data \
                                                            , additional_columns=categorical_columns \
                                                            , excluded_columns=excluded_categorical_columns
                                                            )

            return data


class RawStatisticExtractor(BaseExtractor):
    yaml_tag = u'!RawStatisticExtractor'

    def __init__(self, /,
                 input_files:list
                 , signal:str
                 , alias:str
                 , runId:bool=True
                 , statName:bool=False
                 , statId:bool=False
                 , *args, **kwargs):
        super().__init__(input_files=input_files, *args, **kwargs)

        self.signal:str = signal
        self.alias:str = alias

        self.statName:bool = statName
        self.statId:bool = statId
        self.runId:bool = runId

    def prepare(self):
        data_set = DataSet(self.input_files)

        # For every input file construct a `Delayed` object, a kind of a promise
        # on the data and the leafs of the computation graph
        result_list = []
        for db_file in data_set.get_file_list():
            res = dask.delayed(BaseExtractor.read_statistic_from_file)\
                                         (db_file, self.signal, self.alias
                                          , moduleName = self.moduleName
                                          , statName = self.statName
                                          , statId = self.statId
                                          , runId = self.runId
                                          , categorical_columns = self.categorical_columns
                                          , excluded_categorical_columns = self.categorical_columns_excluded
                                          , base_tags = self.base_tags
                                          , additional_tags = self.additional_tags
                                          , minimal_tags = self.minimal_tags
                                          , attributes_regex_map = self.attributes_regex_map
                                          , iterationvars_regex_map = self.iterationvars_regex_map
                                          , parameters_regex_map = self.parameters_regex_map
                                          )
            attributes = DataAttributes(source_file=db_file, alias=self.alias)
            result_list.append((res, attributes))

        return result_list


class RawScalarExtractor(BaseExtractor):
    yaml_tag = u'!RawScalarExtractor'

    def __init__(self, /,
                 input_files:list
                 , signal:str
                 , alias:str
                 , runId:bool=True
                 , scalarName:bool=False
                 , scalarId:bool=False
                 , *args, **kwargs
                 ):
        super().__init__(input_files=input_files, *args, **kwargs)

        self.signal:str = signal
        self.alias:str = alias

        self.runId:bool = runId
        self.scalarName:bool = scalarName
        self.scalarId:bool = scalarId

    def prepare(self):
        data_set = DataSet(self.input_files)

        # For every input file construct a `Delayed` object, a kind of a promise
        # on the data and the leafs of the computation graph
        result_list = []
        for db_file in data_set.get_file_list():
            res = dask.delayed(BaseExtractor.read_scalars_from_file)\
                                         (db_file, self.signal, self.alias
                                          , moduleName = self.moduleName
                                          , scalarName = self.scalarName
                                          , scalarId = self.scalarId
                                          , runId = self.runId
                                          , categorical_columns = self.categorical_columns
                                          , excluded_categorical_columns = self.categorical_columns_excluded
                                          , base_tags = self.base_tags
                                          , additional_tags = self.additional_tags
                                          , minimal_tags = self.minimal_tags
                                          , attributes_regex_map = self.attributes_regex_map
                                          , iterationvars_regex_map = self.iterationvars_regex_map
                                          , parameters_regex_map = self.parameters_regex_map
                                          )
            attributes = DataAttributes(source_file=db_file, alias=self.alias)
            result_list.append((res, attributes))

        return result_list


class RawExtractor(BaseExtractor):
    yaml_tag = u'!RawExtractor'

    def __init__(self, /,
                 input_files:list
                 , signal:str
                 , alias:str
                 , *args, **kwargs):
        super().__init__(input_files=input_files, *args, **kwargs)

        self.signal:str = signal
        self.alias:str = alias

    def prepare(self):
        data_set = DataSet(self.input_files)

        # For every input file construct a `Delayed` object, a kind of a promise
        # on the data and the leafs of the computation graph
        result_list = []
        for db_file in data_set.get_file_list():
            res = dask.delayed(BaseExtractor.read_signals_from_file)\
                                         (db_file, self.signal, self.alias
                                          , moduleName = self.moduleName
                                          , eventNumber = self.eventNumber
                                          , simtimeRaw = self.simtimeRaw
                                          , categorical_columns = self.categorical_columns
                                          , excluded_categorical_columns = self.categorical_columns_excluded
                                          , base_tags = self.base_tags
                                          , additional_tags = self.additional_tags
                                          , minimal_tags = self.minimal_tags
                                          , attributes_regex_map = self.attributes_regex_map
                                          , iterationvars_regex_map = self.iterationvars_regex_map
                                          , parameters_regex_map = self.parameters_regex_map
                                          )
            attributes = DataAttributes(source_file=db_file, alias=self.alias)
            result_list.append((res, attributes))

        return result_list


class PositionExtractor(BaseExtractor):
    yaml_tag = u'!PositionExtractor'

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

            data = BaseExtractor.apply_tags(data, tags, base_tags=base_tags, additional_tags=additional_tags, minimal=minimal_tags)

            # don't categorize the column with the actual data
            excluded_categorical_columns = excluded_categorical_columns.union(set([alias, x_alias, y_alias]))

            # select columns with a small enough set of possible values to
            # convert into `Categorical`
            data = BaseExtractor.convert_columns_to_category(data \
                                                            , additional_columns=categorical_columns \
                                                            , excluded_columns=excluded_categorical_columns
                                                            )

            return data

    def prepare(self):
        data_set = DataSet(self.input_files)

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
                                , restriction=self.restriction
                                , moduleName=self.moduleName
                                , simtimeRaw=self.simtimeRaw
                                , eventNumber=self.eventNumber
                                , categorical_columns=self.categorical_columns \
                                , excluded_categorical_columns=self.categorical_columns_excluded \
                                , base_tags=self.base_tags, additional_tags=self.additional_tags
                                , minimal_tags=self.minimal_tags
                               )
            attributes = DataAttributes(source_file=db_file, alias=self.alias)
            result_list.append((res, attributes))

        return result_list

    def __init__(self, /,
                 input_files:list
                 , x_signal:str, x_alias:str
                 , y_signal:str, y_alias:str
                 , signal:str
                 , alias:str
                 , restriction:Optional[Tuple[float]] = None
                 , *args, **kwargs
                 ):
        super().__init__(input_files=input_files, *args, **kwargs)

        self.x_signal:str = x_signal
        self.x_alias:str = x_alias
        self.y_signal:str = y_signal
        self.y_alias:str = y_alias

        self.signal:str = signal
        self.alias:str = alias

        if restriction and type(restriction) == str:
            self.restriction = eval(restriction)
        else:
            self.restriction = restriction


class MatchingExtractor(BaseExtractor):
    yaml_tag = u'!MatchingExtractor'

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
                            , attributes_regex_map=tag_regex.attributes_regex_map
                            , iterationvars_regex_map=tag_regex.iterationvars_regex_map
                            , parameters_regex_map=tag_regex.parameters_regex_map
                            , moduleName:bool=True
                            , simtimeRaw:bool=True
                            , eventNumber:bool=False
                            ):
        result_list = []
        for signal, alias in signals:
            res = BaseExtractor.read_signals_from_file(db_file, signal, alias \
                                                      , categorical_columns=categorical_columns \
                                                      , excluded_categorical_columns=excluded_categorical_columns
                                                      , base_tags=base_tags, additional_tags=additional_tags
                                                      , minimal_tags=minimal_tags
                                                      , attributes_regex_map=attributes_regex_map
                                                      , iterationvars_regex_map=iterationvars_regex_map
                                                      , parameters_regex_map=parameters_regex_map
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
            result = BaseExtractor.convert_columns_to_category(result
                                                                , additional_columns=categorical_columns \
                                                                , excluded_columns=excluded_categorical_columns
                                                             )
            return result
        else:
            return pd.DataFrame()

    def prepare(self):
        data_set = DataSet(self.input_files)

        # For every input file construct a `Delayed` object, a kind of a promise
        # on the data, and the leafs of the task graph
        result_list = []
        for db_file in data_set.get_file_list():
            # get all signal names that match the given regular expression
            matching_signals_result = dask.delayed(MatchingExtractor.get_matching_signals)(db_file, self.pattern, self.alias_pattern)
            # get the data for the matched signals
            res = dask.delayed(MatchingExtractor.extract_all_signals)(db_file, matching_signals_result
                                                                       , self.categorical_columns,self. categorical_columns_excluded
                                                                       , base_tags=self.base_tags, additional_tags=self.additional_tags
                                                                       , minimal_tags=self.minimal_tags
                                                                       , attributes_regex_map=self.attributes_regex_map
                                                                       , iterationvars_regex_map=self.iterationvars_regex_map
                                                                       , parameters_regex_map=self.parameters_regex_map
                                                                       , simtimeRaw=self.simtimeRaw
                                                                       , moduleName=self.moduleName
                                                                       , eventNumber=self.eventNumber
                                                                       )
            attributes = DataAttributes(source_file=db_file, alias=self.alias)
            result_list.append((res, attributes))

        return result_list


    def __init__(self, /,
                 input_files:list
                 , pattern:str
                 , alias_pattern:str
                 , alias:str
                 , *args, **kwargs
                 ):
        super().__init__(input_files=input_files, *args, **kwargs)

        self.pattern:str = pattern
        self.alias_pattern:str = alias_pattern
        self.alias:str = alias


def register_constructors():
    yaml.add_constructor(u'!RawExtractor', proto_constructor(RawExtractor))
    yaml.add_constructor(u'!RawScalarExtractor', proto_constructor(RawScalarExtractor))
    yaml.add_constructor(u'!RawStatisticExtractor', proto_constructor(RawStatisticExtractor))
    yaml.add_constructor(u'!PositionExtractor', proto_constructor(PositionExtractor))
    yaml.add_constructor(u'!MatchingExtractor', proto_constructor(MatchingExtractor))

