import pathlib
import re

from typing import Union, List

import pandas as pd

def read_from_file(path, file_format='feather'):
    if file_format == 'feather':
        try:
            data = pd.read_feather(path)
        except Exception as e:
            raise Exception(f'Could not read from: {path}\n{e}')
        return data
    elif file_format == 'hdf':
        try:
            data = pd.read_hdf(path)
        except Exception as e:
            raise Exception(f'Could not read from: {path}\n{e}')
        return data

class DataSet:
    def __init__(self, data_path:Union[List[str], str]):
        self.data_path = data_path

        self.data_files = self.expand_data_path()
        if len(self.data_files) == 0:
            raise Exception(f'No input files for path(s): {data_path}\n')

    def get_data_path(self) -> Union[List[str], str]:
        return self.data_path

    def get_file_list(self) -> List[str]:
        return self.data_files

    def expand_data_path(self):
        file_list = []
        if type(self.data_path) == list:
            for entry in self.data_path:
                files = self.evaluate_regex_path(entry)
                file_list.extend(files)
        else:
            file_list = self.evaluate_regex_path(self.data_path)

        return file_list

    @staticmethod
    def evaluate_regex_path(data_path:str) -> List[str]:
        """
        Take the given path to a directory plus a regular expresion
        """
        data_files = []
        path = pathlib.Path(data_path)
        directory = path.parent
        regex = re.compile(str(path))
        for filename in directory.iterdir():
            if regex.match(fn:=str(filename)):
                data_files.append(fn)
        return data_files
