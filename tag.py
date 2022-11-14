from typing import List, Mapping, Dict, Tuple

class Tag():
    r"""
    A class for adding metadata to the output data

    Parameters
    ----------
    mapping : Mapping[str, object]
        the (key, value) mapping
    """

    def __init__(self, mapping:Mapping[str, object]):
        self.tag = mapping

    def get_mapping(self) -> Mapping[str, object]:
        r"""
        Return the `Mapping` object holding the metadata

        Returns
        -------
        Mapping[str, object]
            the encapsulated (key, value) mapping
        """
        return self.tag

    def get_key_value(self) -> Tuple[str, object]:
        r"""
        Return the metadata contained in this tag as (key, value) tuple

        Returns
        -------
        tuple
            the encapsulated (key, value) pair
        """
        key = list(self.tag)[0]
        value = self.tag[key]
        return key, value

    def __str__(self) -> str:
        r"""
        Return a string representation of the tag

        Returns
        -------
        str
            a string representation of the tag
        """
        key, value = self.get_key_value()
        result = "{}='{}'".format(key, value)
        return result

    def __repr__(self) -> str:
        r"""
        Return a string representation of the tag

        Returns
        -------
        str
            a string representation of the tag
        """
        key, value = self.get_key_value()
        result = "{}={}".format(key, value)
        return result

    @staticmethod
    def get_global_tag():
        r"""
        Return a tag indicating the associated data is generated over a whole run

        Returns
        -------
        Tag
            the tag
        """
        return Tag({ 'global': True })

    # @staticmethod
    # def get_bounded_tag(bounding_box:BoundingBox):
    #     r"""
    #     Return a tag indicating the associated data is generated within a bounding

    #     Parameters
    #     ----------
    #     bounding_box : BoundingBox
    #         the bounding  box

    #     Returns
    #     -------
    #     Tag
    #         the tag
    #     """
    #     return Tag({ 'bounding_box': bounding_box })


    @staticmethod
    def get_module_tag(module:str):
        r"""
        Return a tag indicating the associated data is generated for a module

        Parameters
        ----------
        module : str
            the module of the input data

        Returns
        -------
        Tag
            the tag
        """
        return Tag({ 'module': module })

    @staticmethod
    def get_roadtype_tag(roadtype:float):
        r"""
        Return a tag indicating the associated data is generated for a roadtype

        Parameters
        ----------
        roadtype : float
            the roadtype

        Returns
        -------
        Tag
            the tag
        """
        return Tag({ 'roadtype': roadtype })

    @staticmethod
    def get_dcc_state_tag(dcc_state:int):
        r"""
        Return a tag indicating the associated data is generated for a DCC state

        Parameters
        ----------
        dcc_state : int
            the DCC state

        Returns
        -------
        Tag
            the tag
        """
        return Tag({ 'dcc_state': dcc_state })

    @staticmethod
    def get_scale_tag(scale:float):
        r"""
        Return a tag indicating the associated data is scaled by this factor

        Parameters
        ----------
        scale : float
            the scaling factor

        Returns
        -------
        Tag
            the tag
        """
        return Tag({ 'scale': scale })


