#!/usr/bin/python3

import re

from tag import Tag


class ExtractRunParametersTagsOperation():
    r"""
    Extract the run parameters of from a database and create a `Tag` object for each of them
    """

    @staticmethod
    def add_raw_tags_for_row(row, key_column, value_column, tags):
        r"""
        """
        key = row[key_column]
        value = row[value_column]
        tags.append(Tag({key: value}))


    @staticmethod
    def add_regex_matches_for_row(row, key_column, value_column, regex_map, tags):
        r"""
        """
        key = row[key_column]
        value = row[value_column]
        for tag_key in regex_map:
            for pair in regex_map[tag_key]:
                regex = pair['regex']
                transform = pair['transform']
                match = re.search(regex, key)
                if match:
                    transformed_value = transform(value)
                    tags.append(Tag({tag_key: transformed_value}))


    @staticmethod
    def add_tags(data, key_column, value_column, regex_map, tags):
        r"""
        Create run-specific parameters & attributes
        """
        def add_raw(row):
            ExtractRunParametersTagsOperation.add_raw_tags_for_row(row, key_column, value_column, tags)

        data.apply(add_raw, axis=1)

        def add_matches(row):
            ExtractRunParametersTagsOperation.add_regex_matches_for_row(row, key_column, value_column, regex_map, tags)

        data.apply(add_matches, axis=1)


    @staticmethod
    def add_parameters(data, tags, parameters_regex_map):
        ExtractRunParametersTagsOperation.add_tags(data, 'paramKey', 'paramValue', parameters_regex_map, tags)


    @staticmethod
    def add_iterationvars(data, tags, iterationvars_regex_map):
        iterationvars_value = data[data['attrName'] == 'iterationvars']['attrValue'].iat[0]

        for tag_key in iterationvars_regex_map:
            for pair in iterationvars_regex_map[tag_key]:
                regex = pair['regex']
                transform = pair['transform']

                match = re.search(regex, iterationvars_value)
                if match:
                    value = match.group(0)
                    transformed_value = transform(value)
                    tags.append(Tag({tag_key: transformed_value}))


    @staticmethod
    def add_attributes(data, tags, attributes_regex_map, iterationvars_regex_map):
        ExtractRunParametersTagsOperation.add_tags(data, 'attrName', 'attrValue', attributes_regex_map, tags)

        # process the `iterationsvars` attribute
        ExtractRunParametersTagsOperation.add_iterationvars(data, tags, iterationvars_regex_map)


    @staticmethod
    def extract_attributes_and_params(parameter_extractor, attribute_extractor
                                      , parameters_regex_map, attributes_regex_map, iterationvars_regex_map):
        r"""
        Parameters
        ----------
        parameter_extractor : Callable
            A function that extracts the contents of the `runParam` table and returns it as a pandas.DataFrame
        attribute_extractor : Callable
            A function that extracts the contents of the `runAttr` table and returns it as a pandas.DataFrame
        attributes_regex_map : dict
            The dictionary containing the definitions for the tags to extract from the `runAttr` table
        iterationvars_regex_map : dict
            The dictionary containing the definitions for the tags to extract from the `iterationvars` attribute
        parameters_regex_map : dict
            The dictionary containing the definitions for the tags to extract from the `runParam` table
        """
        tags:List[Tag] = []

        parameters_data = parameter_extractor()
        ExtractRunParametersTagsOperation.add_parameters(parameters_data, tags, parameters_regex_map)

        attributes_data = attribute_extractor()
        ExtractRunParametersTagsOperation.add_attributes(attributes_data, tags, attributes_regex_map, iterationvars_regex_map)

        return tags

