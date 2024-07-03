import pathlib
import re
import os

from typing import List, Optional, Union

# ---

from common.logging_facilities import logi, loge, logd, logw

# ---

import pandas as pd

def read_from_file(path, file_format='feather', sample:Optional[float]=None, sample_seed:int=23, filter_query:str = None):
    if file_format == 'feather':
        try:
            data = pd.read_feather(path)
            if sample:
                logi(f'sampling {sample*100}% of data from {path}')
                data = data.sample(frac=sample, random_state=sample_seed)
            if filter_query:
                logi(f'filtering data with the query expression "{filter_query}"')
                data.query(filter_query, inplace=True)
        except Exception as e:
            raise Exception(f'Could not read from: {path}\n{e}')
        return data
    elif file_format == 'hdf':
        try:
            data = pd.read_hdf(path)
            if sample:
                logi(f'sampling {sample*100}% of data from {path}')
                data = data.sample(frac=sample, random_state=sample_seed)
            if filter_query:
                logi(f'filtering data with the query expression "{filter_query}"')
                data.query(filter_query, inplace=True)
        except Exception as e:
            raise Exception(f'Could not read from: {path}\n{e}')
        return data

class DataSet:
    def __init__(self, data_path:Union[List[str], str]):
        self.data_path = data_path

        self.common_root, self.data_files = self.expand_data_path()
        if len(self.data_files) == 0:
            raise Exception(f'No input files for path(s): {data_path}\n')

    def get_data_path(self) -> Union[List[str], str]:
        return self.data_path

    def get_file_list(self) -> List[str]:
        return self.data_files

    def get_common_root(self) -> str:
        return self.common_root

    def expand_data_path(self):
        file_list = []
        base_paths = []
        if type(self.data_path) == list:
            for entry in self.data_path:
                base_path, files = self.evaluate_regex_path(entry)
                file_list.extend(files)
                base_paths.append(base_path)
            common_root = os.path.commonprefix(base_paths)
        else:
            common_root, file_list = self.evaluate_regex_path(self.data_path)

        return common_root, file_list

    @staticmethod
    def evaluate_regex_path(data_path:str):
        r"""
        Take the given regular expression to generate a list of paths that match it.

        Parameters
        ----------
        data_path : str
            The regular expression that is used to select files.

        Returns
        -------
        List[str]
            A list of paths matching the regular expression.
        """
        data_files = []

        common_root = DataSet.find_base_path_in_regex(data_path)
        logd(f"determined common root path for input_files: {common_root=}")

        relative_regex = pathlib.Path(data_path).relative_to(common_root)
        logd(f"searching for files with regex {relative_regex=} in root path")

        regex = re.compile(str(relative_regex))
        for root, dirs, files in os.walk(common_root):
            for file in files:
                # Check if the file matches any of the regex patterns
                file_full_path = os.path.join(root, file)
                relative_path = pathlib.Path(file_full_path).relative_to(common_root)
                if regex.search(str(relative_path)):
                    logd(f"adding {file_full_path=}")
                    data_files.append(file_full_path)

        return str(common_root), data_files

    @staticmethod
    def find_base_path_in_regex(path_regex:str) -> str:
        r"""
        Takes a regular expression for a file path and determines the static part of the given path regex.

        Parameters
        ----------
        path_regex : str
            The regex pattern for the file path.

        Returns
        -------
        str
            Static parts of the path that do not contain any regex.
        """
        # Pattern to match literal text within the regex
        # This pattern looks for sequences of characters that are not special regex symbols.
        # It will also match on literals that need to be escaped in a regex, such as parentheses
        # in the path, but those can then be treated as part of the regex.
        base_path_pattern = re.compile(r'[a-zA-Z0-9_\-/]+')

        # Find all static parts in the given regex pattern
        base_path = base_path_pattern.findall(path_regex)
        common_root = pathlib.Path(base_path[0]).parent
        return common_root
