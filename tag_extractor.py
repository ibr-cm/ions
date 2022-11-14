#!/usr/bin/python3

import re

from tag import Tag


class ExtractRunParametersTagsOperation():
    r"""
    Extract the run parameters of the input DB and create `Tags` from them

    Parameters
    ----------
    db_session : DbSession
        the session object for acessing the input DB
    """

    # def __init__(self, **kwds):
    #     pass


    def get_run_parameters(self):
        r"""
        Return key-value pairs of the module parameters for the run

        Returns
        -------
        pandas.DataFrame
            the the key-value pairs
        """
        query = """
            SELECT paramKey, paramValue
            FROM runParam AS r;
            """
        return self.execute_query(query)


    def get_run_attributes(self):
        r"""
        Return key-value pairs of the attributes for the run

        Returns
        -------
        pandas.DataFrame
            the the key-value pairs
        """
        query = """
            SELECT attrName, attrValue
            FROM runAttr AS r;
            """
        return self.execute_query(query)


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
        tags:List[Tag] = []

        parameters_data = parameter_extractor()
        ExtractRunParametersTagsOperation.add_parameters(parameters_data, tags, parameters_regex_map)

        attributes_data = attribute_extractor()
        ExtractRunParametersTagsOperation.add_attributes(attributes_data, tags, attributes_regex_map, iterationvars_regex_map)

        return tags


    # def exec(self):
    #     r"""
    #     """
    #     self.log("----------------- extracting tags from runParam & runAttr ")

    #     tags = extract_attributes_and_params(self.get_run_parameters, self.get_run_attributes)

    #     self.log('-----------------')

    #     return tags


