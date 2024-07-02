
from typing import Optional, Union, List, Set, Tuple

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
    r"""
    A utility class to run a query over a SQLite3 database or to extract the parameters and attributes for a run from a database.

    Parameters
    ----------
    db_file : str
        The path to the SQLite3 database file
    """
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
        r"""
        Parameters
        ----------
        attributes_regex_map : dict
            The dictionary containing the definitions for the tags to extract from the `runAttr` table
        iterationvars_regex_map : dict
            The dictionary containing the definitions for the tags to extract from the `iterationvars` attribute
        parameters_regex_map : dict
            The dictionary containing the definitions for the tags to extract from the `runParam` table

        Extract all tags defined in the given mappings from the `runAttr` and `runParam` tables and parse the value of the `iterationvars` attribute.
        See the module `tag_regular_expressions` for the expected structure of the mappings.
        """
        tags = ExtractRunParametersTagsOperation.extract_attributes_and_params(self.parameter_extractor, self.attribute_extractor
                                                                               , parameters_regex_map, attributes_regex_map, iterationvars_regex_map)
        return tags



class DataAttributes(YAMLObject):
    r"""
    A class for assigning arbitrary attributes to a dataset.
    The constructor accept an arbitrary number of keyword arguments and turns
    them into object attributes.

    Parameters
    ----------
    source_file : str
        The file name the dataset was extracted from
    source_files : List[str]
        The list of file names the dataset was extracted from
    common_root : str
        The root directory that was not containing regex for search files
    alias : List[str]
        The alias given to the data in the dataset
    aliases : List[str]
        The aliases given to the data in the dataset
    """
    def __init__(self, /,  **kwargs):
        self.source_files = set()
        self.aliases = set()

        for key in kwargs:
            if key == 'source_file':
                self.source_files.add(kwargs[key])
            elif key == 'source_files':
                for file in kwargs[key]:
                    self.source_files.add(kwargs[key])
            elif key == 'common_root':
                self.common_root = kwargs[key]
            elif key == 'alias':
                self.aliases.add(kwargs[key])
            elif key == 'aliases':
                for file in kwargs[key]:
                    self.aliases.add(kwargs[key])
            else:
                setattr(self, key, kwargs[key])

    def get_source_files(self) -> Set[str]:
        return self.source_files

    def common_root(self) -> Set[str]:
        return self.common_root

    def add_source_file(self, source_file:str):
        self.source_files.add(source_file)

    def add_source_files(self, source_files:Set[str]):
        for source_file in source_files:
            self.source_files.add(source_file)

    def get_aliases(self) -> Set[str]:
        return self.aliases

    def add_alias(self, alias:str):
        self.aliases.add(alias)

    def remove_source_file(self, source_file:str):
        self.source_files.remove(source_file)

    def __str__(self) -> str:
        return str(self.__dict__)

    def __repr__(self) -> str:
        return str(self.__dict__)

# ----------------------------------------


class Extractor(YAMLObject):
    r"""
    A class for extracting and preprocessing data from a SQLite database.
    This is the abstract base class.
    """

    yaml_tag = u'!Extractor'

    def prepare(self):
        r"""
        Prepare and return a list or a single dask.Delayed task
        """
        return None

    def set_tag_maps(self, attributes_regex_map, iterationvars_regex_map, parameters_regex_map):
        setattr(self, 'attributes_regex_map', attributes_regex_map)
        setattr(self, 'iterationvars_regex_map', iterationvars_regex_map)
        setattr(self, 'parameters_regex_map', parameters_regex_map)

class BaseExtractor(Extractor):

    yaml_tag = u'!BaseExtractor'

    def __init__(self, /,
                 input_files:list[str]
                 , categorical_columns:Optional[list[str]] = None
                 , numerical_columns:Optional[Union[dict[str, str], list[str]]] = None
                 , *args, **kwargs
                 ):
        self.input_files:list = list(input_files)

        # categorical_columns and numerical_columns (if appropriate) are explicitly converted
        # to a set to alleviate the need for an explicit tag in the YAML recipe, since pyyaml
        # always interprets values in curly braces as dictionaries
        if not categorical_columns:
            self.categorical_columns:set[str] = set()
        else:
            self.categorical_columns:set[str] = set(categorical_columns)
        if not numerical_columns:
            self.numerical_columns:set[str] = set()
        else:
            if not isinstance(numerical_columns, dict):
                self.numerical_columns:set[str] = set(numerical_columns)
            else:
                self.numerical_columns:dict[str, str] = dict(numerical_columns)


    @staticmethod
    def convert_columns_dtype(data:pd.DataFrame
                            , categorical_columns:Optional[set[str]] = None
                            , numerical_columns:Optional[Union[dict[str, str], set[str]]] = None):
        r"""
        Convert the data in the specified columns of the given DataFrame to either a
        `categorical data type <https://pandas.pydata.org/docs/user_guide/categorical.html>`_ or a
        `numerical data type <https://pandas.pydata.org/pandas-docs/stable/user_guide/basics.html#basics-dtypes>`_

        Parameters
        ----------
        data: pd.DataFrame
            The input DataFrame whose columns are to be converted.

        categorical_columns: set
            The set of names of the columns to convert to a categorical data type.

        numerical_columns: Union[dict[str, str], set[str]]
            The set of names of the columns to convert to a categorical data type.
            The data type to convert to can also be given explicitly as a dictionary
            with the column names as keys and the data type as values.
        """
        if not categorical_columns:
            categorical_columns = frozenset()
        if not numerical_columns:
            numerical_columns = frozenset()

        # get the set of columns actually present in the DataFrame
        actual_categorical_columns = categorical_columns.intersection(data.columns)
        missing_categorical_columns = categorical_columns.difference(actual_categorical_columns)
        if len(missing_categorical_columns) > 0:
            logw(f"columns for conversion to categorical data types not found in DataFrame: {missing_categorical_columns}")

        # if the numerical data types are explicitly given as a dictionary, use them
        if isinstance(numerical_columns, dict):
            numerical_columns_set = set(numerical_columns.keys())
            numerical_columns_dict = numerical_columns
        else:
            # if the numerical data types are not explicitly given, convert to float
            numerical_columns_set = numerical_columns
            numerical_columns_dict = {}
            for column in numerical_columns_set:
                numerical_columns_dict[column] = 'float'

        actual_numerical_columns = numerical_columns_set.intersection(data.columns)
        missing_numerical_columns = numerical_columns_set.difference(actual_numerical_columns)
        if len(missing_numerical_columns) > 0:
            logw(f"columns for conversion to numerical data types not found in DataFrame: {missing_numerical_columns}")

        logd(f"columns to convert to categorical data types: {actual_categorical_columns}")

        def key_extractor(obj):
            def is_float(string):
                return string.replace('.', '').isnumeric()

            def convert_numerical(num_str):
                if num_str.isnumeric():
                    # convert to int if it's numerical
                    return int(num_str)
                elif is_float(num_str):
                    # convert to float if it's numerical when a period is removed
                    return float(num_str)
                elif num_str.isalpha():
                    # not a numerical
                    return num_str

            if isinstance(obj, str):
                # characters that are used as separators in e.g. variable names
                separator_map = { '-':'', '_':'', ':':'', ' ':'' }
                if obj.isalpha():
                    return obj
                elif obj.isnumeric():
                    return int(obj)
                elif is_float(obj):
                    return float(obj)
                elif obj.translate(str.maketrans(separator_map)).isalnum():
                    # obj is alpha-numerical with possible extra ascii characters used as separators
                    # split the string into characters and numerical literals
                    regex = re.compile(r'[^\W\d_]+|\d+')
                    split_str = regex.findall(obj)
                    for i in range(0, len(split_str)):
                        split_str[i] = convert_numerical(split_str[i])
                    return tuple(split_str)
                else:
                    return obj
            else:
                # return object as is for int, float or other
                return obj

        for column in actual_categorical_columns:
            data[column] = data[column].astype('category')
            sorted_categories = sorted(data[column].cat.categories, key=key_extractor)
            data[column] = data[column].astype(pd.CategoricalDtype(categories=sorted_categories, ordered=True))
            logd(f'{column=} {data[column].dtype=}')

        logd(f"columns to convert to numerical data types: {actual_numerical_columns}")
        for column in actual_numerical_columns:
            data[column] = data[column].astype(numerical_columns_dict[column])

        return data


    @staticmethod
    def read_sql_from_file(db_file
                            , query
                            , includeFilename=False
                            ):
            sql_reader = SqlLiteReader(db_file)

            try:
                data = sql_reader.execute_sql_query(query)
            except Exception as e:
                loge(f'>>>> ERROR: no data could be extracted from {db_file}:\n {e}')
                return pd.DataFrame()

            if 'rowId' in data.columns:
                data = data.drop(labels=['rowId'], axis=1)

            if (data.empty):
                logw(f'Extractor: extraction yields no data for {db_file}')
                return pd.DataFrame()

            # add path to dataframe
            if (includeFilename):
                data["filename"] = str(db_file)

            return data


class SqlExtractor(BaseExtractor):
    r"""
    Extract the data from files using a SQL statement

    Parameters
    ----------
    input_files: List[str]
        the list of paths to the input files, as literal path or as a regular expression

    query: str
        the name of the signal which is to be extracted

    """
    yaml_tag = u'!SqlExtractor'

    def __init__(self, /,
                 input_files:list
                 , query:str
                 , includeFilename:bool = False
                 , *args, **kwargs):
        super().__init__(input_files=input_files, *args, **kwargs)

        self.query:str = query
        self.includeFilename:bool = includeFilename


    def prepare(self):
        data_set = DataSet(self.input_files)

        # For every input file construct a `Delayed` object, a kind of a promise
        # on the data and the leafs of the computation graph
        result_list = []
        for db_file in data_set.get_file_list():
            res = dask.delayed(SqlExtractor.read_query_from_file)\
                                         (db_file, self.query
                                          , includeFilename=self.includeFilename
                                          , categorical_columns = self.categorical_columns
                                          , numerical_columns = self.numerical_columns
                                          )
            attributes = DataAttributes(source_file=db_file, common_root=data_set.get_common_root())
            result_list.append((res, attributes))

        return result_list

    @staticmethod
    def read_query_from_file(db_file
                            , query
                            , includeFilename=False
                            , categorical_columns:Optional[set[str]] = None
                            , numerical_columns:Optional[Union[dict[str, str], set[str]]] = None
                            ):
            data = BaseExtractor.read_sql_from_file(db_file, query, includeFilename)

            # convert the data type of the explicitly named columns
            data = BaseExtractor.convert_columns_dtype(data \
                                                      , categorical_columns = categorical_columns \
                                                      , numerical_columns = numerical_columns
                                                      )
            return data

class OmnetExtractor(BaseExtractor):
    r"""
    A class for extracting and preprocessing data from a SQLite database.
    This is the base class.
    """

    yaml_tag = u'!OmnetExtractor'

    def __init__(self, /,
                 input_files:list
                 , categorical_columns:Optional[set[str]] = None
                 , numerical_columns:Optional[Union[dict[str, str], set[str]]] = None
                 , base_tags:Optional[List] = None
                 , additional_tags:Optional[list[str]] = None
                 , minimal_tags:bool = True
                 , simtimeRaw:bool = True
                 , moduleName:bool = True
                 , eventNumber:bool = True
                 , *args, **kwargs
                 ):

        super().__init__(input_files=input_files
                         , categorical_columns = categorical_columns
                         , numerical_columns = numerical_columns
                         , *args, **kwargs)

        if base_tags != None:
            self.base_tags:list = base_tags
        else:
            if minimal_tags:
                self.base_tags = BASE_TAGS_EXTRACTION_MINIMAL
            else:
                self.base_tags = BASE_TAGS_EXTRACTION_FULL

        if additional_tags:
            self.additional_tags:list = list(additional_tags)
        else:
            self.additional_tags:list = list()

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

        return OmnetExtractor.read_query_from_file(db_file, query, alias, **kwargs)


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

        return OmnetExtractor.read_query_from_file(db_file, query, alias, **kwargs)


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

        return OmnetExtractor.read_query_from_file(db_file, query, alias, **kwargs)


    @staticmethod
    def read_pattern_matched_signals_from_file(db_file, pattern, alias
                               , vectorName:bool=True
                               , simtimeRaw:bool=True
                               , moduleName:bool=True
                               , eventNumber:bool=True
                               , **kwargs):
        query = sql_queries.generate_signal_like_query(pattern, value_label=alias
                                                  , vectorName=vectorName
                                                  , moduleName=moduleName
                                                  , simtimeRaw=simtimeRaw
                                                  , eventNumber=eventNumber)

        return OmnetExtractor.read_query_from_file(db_file, query, alias, **kwargs)


    @staticmethod
    def read_pattern_matched_scalars_from_file(db_file, pattern, alias
                               , scalarName:bool=True
                               , moduleName:bool=True
                               , scalarId:bool=False
                               , runId:bool=False
                               , **kwargs):
        query = sql_queries.generate_scalar_like_query(pattern, value_label=alias
                                                  , scalarName=scalarName
                                                  , moduleName=moduleName
                                                  , scalarId=scalarId
                                                  , runId=runId)

        return OmnetExtractor.read_query_from_file(db_file, query, alias, **kwargs)


    @staticmethod
    def read_query_from_file(db_file, query, alias
                               , categorical_columns:Optional[set[str]]=None
                               , numerical_columns:Optional[Union[dict[str,str], set[str]]]=None
                               , base_tags = None, additional_tags = None
                               , minimal_tags=True
                               , simtimeRaw=True
                               , moduleName=True
                               , eventNumber=True
                               , includeFilename=False
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

            data = BaseExtractor.read_sql_from_file(db_file
                                                    , query \
                                                    , includeFilename
                                                    )

            data = OmnetExtractor.apply_tags(data, tags, base_tags=base_tags, additional_tags=additional_tags, minimal=minimal_tags)

            # convert the data type of the explicitly named columns
            data = BaseExtractor.convert_columns_dtype(data \
                                                      , categorical_columns = categorical_columns \
                                                      , numerical_columns = numerical_columns
                                                      )

            return data


class RawStatisticExtractor(OmnetExtractor):
    r"""
    Extract the data for a signal from the `statistic` table of the input files specified.

    Parameters
    ----------
    input_files: List[str]
        the list of paths to the input files, as literal path or as a regular expression

    signal: str
        the name of the signal which is to be extracted

    alias: str
        the name given to the column with the extracted signal data

    runId: str
        whether to extract the `runId` column as well

    statName: str
        whether to extract the `statName` column as well

    statId: str
        whether to extract the `statId` column as well
    """
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
            res = dask.delayed(OmnetExtractor.read_statistic_from_file)\
                                         (db_file, self.signal, self.alias
                                          , moduleName = self.moduleName
                                          , statName = self.statName
                                          , statId = self.statId
                                          , runId = self.runId
                                          , categorical_columns = self.categorical_columns
                                          , numerical_columns = self.numerical_columns
                                          , base_tags = self.base_tags
                                          , additional_tags = self.additional_tags
                                          , minimal_tags = self.minimal_tags
                                          , attributes_regex_map = self.attributes_regex_map
                                          , iterationvars_regex_map = self.iterationvars_regex_map
                                          , parameters_regex_map = self.parameters_regex_map
                                          )
            attributes = DataAttributes(source_file=db_file, alias=self.alias, common_root=data_set.get_common_root())
            result_list.append((res, attributes))

        return result_list


class RawScalarExtractor(OmnetExtractor):
    r"""
    Extract the data for a signal from the `scalar` table of the input files specified.

    Parameters
    ----------
    input_files: List[str]
        the list of paths to the input files, as literal path or as a regular expression

    signal: str
        the name of the signal which is to be extracted

    alias: str
        the name given to the column with the extracted signal data

    runId: str
        whether to extract the `runId` column as well

    scalarName: str
        whether to extract the `scalarName` column as well

    scalarId: str
        whether to extract the `scalarId` column as well
    """
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
            res = dask.delayed(OmnetExtractor.read_scalars_from_file)\
                                         (db_file, self.signal, self.alias
                                          , moduleName = self.moduleName
                                          , scalarName = self.scalarName
                                          , scalarId = self.scalarId
                                          , runId = self.runId
                                          , categorical_columns = self.categorical_columns
                                          , numerical_columns = self.numerical_columns
                                          , base_tags = self.base_tags
                                          , additional_tags = self.additional_tags
                                          , minimal_tags = self.minimal_tags
                                          , attributes_regex_map = self.attributes_regex_map
                                          , iterationvars_regex_map = self.iterationvars_regex_map
                                          , parameters_regex_map = self.parameters_regex_map
                                          )
            attributes = DataAttributes(source_file=db_file, alias=self.alias, common_root=data_set.get_common_root())
            result_list.append((res, attributes))

        return result_list


class RawExtractor(OmnetExtractor):
    r"""
    Extract the data for a signal from the input files specified.

    Parameters
    ----------
    input_files: List[str]
        the list of paths to the input files, as literal path or as a regular expression

    signal: str
        the name of the signal which is to be extracted

    alias: str
        the name given to the column with the extracted signal data
    """
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
            res = dask.delayed(OmnetExtractor.read_signals_from_file)\
                                         (db_file, self.signal, self.alias
                                          , moduleName = self.moduleName
                                          , eventNumber = self.eventNumber
                                          , simtimeRaw = self.simtimeRaw
                                          , categorical_columns = self.categorical_columns
                                          , numerical_columns = self.numerical_columns
                                          , base_tags = self.base_tags
                                          , additional_tags = self.additional_tags
                                          , minimal_tags = self.minimal_tags
                                          , attributes_regex_map = self.attributes_regex_map
                                          , iterationvars_regex_map = self.iterationvars_regex_map
                                          , parameters_regex_map = self.parameters_regex_map
                                          )
            attributes = DataAttributes(source_file=db_file, alias=self.alias, common_root=data_set.get_common_root())
            result_list.append((res, attributes))

        return result_list


class PositionExtractor(OmnetExtractor):
    r"""
    Extract the data for a signal, with the associated positions, from the input files specified.

    Parameters
    ----------
    input_files: List[str]
        the list of paths to the input files, as literal path or as a regular expression

    x_signal: str
        the name of the signal with the x-axis coordinates

    x_alias: str
        the name given to the column with the extracted x-axis position data

    y_signal: str
        the name of the signal with the y-axis coordinates

    y_alias: str
        the name given to the column with the extracted y-axis position data

    signal: str
        the name of the signal to extract

    alias: str
        the name given to the column with the extracted signal data

    restriction: Optional[Union[Tuple[float], str]]
        this defines a area restriction on the positions from which the signal
        data is extracted, the tuple (x0, y0, x1, y1) defines the corners of
        a rectangle
    """
    yaml_tag = u'!PositionExtractor'

    def __init__(self, /,
                 input_files:list
                 , x_signal:str, x_alias:str
                 , y_signal:str, y_alias:str
                 , signal:str
                 , alias:str
                 , restriction:Optional[Union[Tuple[float], str]] = None
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
                               , categorical_columns:Optional[set[str]]=None
                               , numerical_columns:Optional[Union[dict[str,str], set[str]]]=None
                               , base_tags = None, additional_tags = None
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

            data = OmnetExtractor.apply_tags(data, tags, base_tags=base_tags, additional_tags=additional_tags, minimal=minimal_tags)

            # convert the data type of the explicitly named columns
            data = OmnetExtractor.convert_columns_dtype(data \
                                                       , categorical_columns = categorical_columns \
                                                       , numerical_columns = numerical_columns
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
                                , numerical_columns=self.numerical_columns \
                                , base_tags=self.base_tags, additional_tags=self.additional_tags
                                , minimal_tags=self.minimal_tags
                               )
            attributes = DataAttributes(source_file=db_file, alias=self.alias, common_root=data_set.get_common_root())
            result_list.append((res, attributes))

        return result_list


class MatchingExtractor(OmnetExtractor):
    r"""
    Extract the data for multiple signals matching a regular expression, with
    the associated positions, from the input files specified.

    Parameters
    ----------
    input_files: List[str]
        the list of paths to the input files, as literal path or as a regular expression

    pattern: str
        the regular expression used for matching possible signal names

    alias_pattern: str
        the template string for naming the extracted signal

    alias: str
        the name given to the column with the extracted signal data
    """
    yaml_tag = u'!MatchingExtractor'

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
                            , categorical_columns:Optional[set[str]]=None
                            , numerical_columns:Optional[Union[dict[str,str], set[str]]]=None
                            , base_tags=None, additional_tags=None
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
            res = OmnetExtractor.read_signals_from_file(db_file, signal, alias \
                                                      , categorical_columns=categorical_columns \
                                                      , numerical_columns=numerical_columns
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
            # convert the data type of the explicitly named columns
            result = OmnetExtractor.convert_columns_dtype(result
                                                         , categorical_columns=categorical_columns \
                                                         , numerical_columns=numerical_columns
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
                                                                       , self.categorical_columns
                                                                       , self.numerical_columns
                                                                       , base_tags=self.base_tags, additional_tags=self.additional_tags
                                                                       , minimal_tags=self.minimal_tags
                                                                       , attributes_regex_map=self.attributes_regex_map
                                                                       , iterationvars_regex_map=self.iterationvars_regex_map
                                                                       , parameters_regex_map=self.parameters_regex_map
                                                                       , simtimeRaw=self.simtimeRaw
                                                                       , moduleName=self.moduleName
                                                                       , eventNumber=self.eventNumber
                                                                       )
            attributes = DataAttributes(source_file=db_file, alias=self.alias, common_root=data_set.get_common_root())
            result_list.append((res, attributes))

        return result_list


class PatternMatchingBulkExtractor(OmnetExtractor):
    r"""
    Extract the data for multiple signals matching a SQL LIKE pattern
    expression from the input files specified.

    Parameters
    ----------
    input_files: List[str]
        the list of paths to the input files, as literal path or as a regular expression

    pattern: str
        the SQL LIKE pattern matching expression used for matching on possible signal names

    alias: str
        the name given to the column with the extracted signal data

    alias_match_pattern: str
        the regular expression used for extracting named capture groups from the matched signal names

    alias_pattern: str
        the template string for naming the extracted signal from the named capture groups matched by alias_match_pattern

    """
    yaml_tag = u'!PatternMatchingBulkExtractor'

    def __init__(self, /,
                 input_files:list
                 , pattern:str
                 , alias:str
                 , alias_match_pattern:str
                 , alias_pattern:str
                 , *args, **kwargs
                 ):
        super().__init__(input_files=input_files, *args, **kwargs)

        self.pattern:str = pattern
        self.alias:str = alias

        self.alias_match_pattern:str = alias_match_pattern
        self.alias_pattern:str = alias_pattern

    @staticmethod
    def extract_all_signals(db_file, pattern, alias
                            , alias_match_pattern:str, alias_pattern:str
                            , categorical_columns:Optional[set[str]]=None
                            , numerical_columns:Optional[Union[dict[str,str], set[str]]]=None
                            , base_tags=None, additional_tags=None
                            , minimal_tags=True
                            , attributes_regex_map=tag_regex.attributes_regex_map
                            , iterationvars_regex_map=tag_regex.iterationvars_regex_map
                            , parameters_regex_map=tag_regex.parameters_regex_map
                            , vectorName:bool=True
                            , moduleName:bool=True
                            , simtimeRaw:bool=True
                            , eventNumber:bool=False
                            ):
        data = OmnetExtractor.read_pattern_matched_signals_from_file(db_file, pattern, alias \
                                                      , categorical_columns=categorical_columns \
                                                      , numerical_columns=numerical_columns
                                                      , base_tags=base_tags, additional_tags=additional_tags
                                                      , minimal_tags=minimal_tags
                                                      , attributes_regex_map=attributes_regex_map
                                                      , iterationvars_regex_map=iterationvars_regex_map
                                                      , parameters_regex_map=parameters_regex_map
                                                      , vectorName=vectorName
                                                      , simtimeRaw=simtimeRaw
                                                      , moduleName=moduleName
                                                      , eventNumber=eventNumber
                                                     )


        def process_vectorName(d):
            # compile the signal matching regex
            regex = re.compile(alias_match_pattern)
            r = regex.search(d)
            if r:
                # construct the new name by substituting the matched and bound variables
                alias = alias_pattern.format(**r.groupdict())
                return alias
            return d

        try:
            data['variable'] = data['vectorName'].apply(process_vectorName)
        except Exception as e:
            loge(f"error assigning the variable name")
            loge(f'=<=<=  {db_file=}')
            loge(f'=<=<=  {data=}')

        data = data.drop(['vectorName'], axis=1)

        if not data is None and not data.empty:
            # convert the data type of the explicitly named columns
            result = OmnetExtractor.convert_columns_dtype(data
                                                         , categorical_columns=categorical_columns \
                                                         , numerical_columns=numerical_columns
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
            # get the data for all signals that match the given SQL pattern
            res = dask.delayed(PatternMatchingBulkExtractor.extract_all_signals)(db_file, self.pattern, self.alias
                                                                       , self.alias_match_pattern, self.alias_pattern
                                                                       , self.categorical_columns
                                                                       , self.numerical_columns
                                                                       , base_tags=self.base_tags, additional_tags=self.additional_tags
                                                                       , minimal_tags=self.minimal_tags
                                                                       , attributes_regex_map=self.attributes_regex_map
                                                                       , iterationvars_regex_map=self.iterationvars_regex_map
                                                                       , parameters_regex_map=self.parameters_regex_map
                                                                       , vectorName=True
                                                                       , simtimeRaw=self.simtimeRaw
                                                                       , moduleName=self.moduleName
                                                                       , eventNumber=self.eventNumber
                                                                       )
            attributes = DataAttributes(source_file=db_file, alias=self.alias, common_root=data_set.get_common_root())
            result_list.append((res, attributes))

        return result_list


class PatternMatchingBulkScalarExtractor(OmnetExtractor):
    r"""
    Extract the data for multiple scalars matching a SQL LIKE pattern
    expression from the input files specified.
    Equivalent to:
      SELECT * FROM scalar WHERE scalarName LIKE <pattern>;

    Parameters
    ----------
    input_files: List[str]
        the list of paths to the input files, as literal path or as a regular expression

    pattern: str
        the SQL LIKE pattern matching expression used for matching on possible signal names

    alias: str
        the name given to the column with the extracted signal data

    alias_match_pattern: str
        the regular expression used for extracting named capture groups from the matched signal names

    alias_pattern: str
        the template string for naming the extracted signal from the named capture groups matched by alias_match_pattern

    runId: bool
        whether to extract the runId column

    scalarId: bool
        whether to extract the scalarId column

    scalarName: bool
        whether to extract the scalarName column
    """
    yaml_tag = u'!PatternMatchingBulkScalarExtractor'

    def __init__(self, /,
                 input_files:list
                 , pattern:str
                 , alias:str
                 , alias_match_pattern:str
                 , alias_pattern:str
                 , scalarName:bool = True
                 , scalarId:bool = False
                 , runId:bool = False
                 , *args, **kwargs
                 ):
        super().__init__(input_files=input_files, *args, **kwargs)

        self.pattern:str = pattern
        self.alias:str = alias

        self.alias_match_pattern:str = alias_match_pattern
        self.alias_pattern:str = alias_pattern

        self.runId:bool = runId
        self.scalarId:bool = runId
        self.scalarName:bool = runId

    @staticmethod
    def extract_all_scalars(db_file, pattern, alias
                            , alias_match_pattern:str, alias_pattern:str
                            , categorical_columns:Optional[set[str]]=None
                            , numerical_columns:Optional[Union[dict[str,str], set[str]]]=None
                            , base_tags=None, additional_tags=None
                            , minimal_tags=True
                            , attributes_regex_map=tag_regex.attributes_regex_map
                            , iterationvars_regex_map=tag_regex.iterationvars_regex_map
                            , parameters_regex_map=tag_regex.parameters_regex_map
                            , scalarName:bool=True
                            , scalarId:bool=True
                            , moduleName:bool=True
                            , runId:bool=False
                            ):
        data = OmnetExtractor.read_pattern_matched_scalars_from_file(db_file, pattern, alias \
                                                      , categorical_columns=categorical_columns \
                                                      , numerical_columns=numerical_columns
                                                      , base_tags=base_tags, additional_tags=additional_tags
                                                      , minimal_tags=minimal_tags
                                                      , attributes_regex_map=attributes_regex_map
                                                      , iterationvars_regex_map=iterationvars_regex_map
                                                      , parameters_regex_map=parameters_regex_map
                                                      , scalarName=scalarName
                                                      , scalarId=scalarId
                                                      , moduleName=moduleName
                                                      , runId=runId
                                                     )

        print(f'{data=}')
        if data is None or (not data is None and data.empty):
            return pd.DataFrame()

        def process_vectorName(d):
            # compile the signal matching regex
            regex = re.compile(alias_match_pattern)
            r = regex.search(d)
            if r:
                # construct the new name by substituting the matched and bound variables
                alias = alias_pattern.format(**r.groupdict())
                return alias
            return d

        try:
            data['variable'] = data['scalarName'].apply(process_vectorName)
        except Exception as e:
            loge(f"error assigning the variable name")
            loge(f'=<=<=  {e=}')
            loge(f'=<=<=  {db_file=}')
            loge(f'=<=<=  {data=}')

        data = data.drop(['scalarName'], axis=1)

        if not data is None and not data.empty:
            # convert the data type of the explicitly named columns
            result = OmnetExtractor.convert_columns_dtype(data
                                                         , categorical_columns=categorical_columns \
                                                         , numerical_columns=numerical_columns
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
            # get the data for all signals that match the given SQL pattern
            res = dask.delayed(PatternMatchingBulkScalarExtractor.extract_all_scalars)(db_file, self.pattern, self.alias
                                                                       , self.alias_match_pattern, self.alias_pattern
                                                                       , self.categorical_columns
                                                                       , self.numerical_columns
                                                                       , base_tags=self.base_tags, additional_tags=self.additional_tags
                                                                       , minimal_tags=self.minimal_tags
                                                                       , attributes_regex_map=self.attributes_regex_map
                                                                       , iterationvars_regex_map=self.iterationvars_regex_map
                                                                       , parameters_regex_map=self.parameters_regex_map
                                                                       , scalarName=True
                                                                       , scalarId=self.scalarId
                                                                       , moduleName=self.moduleName
                                                                       , runId=self.runId
                                                                       )
            attributes = DataAttributes(source_file=db_file, alias=self.alias, common_root=data_set.get_common_root())
            result_list.append((res, attributes))

        return result_list


def register_constructors():
    r"""
    Register YAML constructors for all extractors
    """
    yaml.add_constructor(u'!RawExtractor', proto_constructor(RawExtractor))
    yaml.add_constructor(u'!RawScalarExtractor', proto_constructor(RawScalarExtractor))
    yaml.add_constructor(u'!RawStatisticExtractor', proto_constructor(RawStatisticExtractor))
    yaml.add_constructor(u'!PositionExtractor', proto_constructor(PositionExtractor))
    yaml.add_constructor(u'!MatchingExtractor', proto_constructor(MatchingExtractor))
    yaml.add_constructor(u'!PatternMatchingBulkExtractor', proto_constructor(PatternMatchingBulkExtractor))
    yaml.add_constructor(u'!PatternMatchingBulkScalarExtractor', proto_constructor(PatternMatchingBulkScalarExtractor))
    yaml.add_constructor(u'!SqlExtractor', proto_constructor(SqlExtractor))

