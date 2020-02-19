# -*- coding: utf-8 -*-
"""
Created by Pierluigi on 2020-02-02
project: elasticizefiles
"""
from abc import ABCMeta
from abc import abstractmethod
from elasticizefiles.utils import to_snake


class Extractor(metaclass=ABCMeta):

    def __init__(self, name=None, **kwargs):
        self.name = name
        if name is None:
            self.name = to_snake(self.__class__.__name__)

    @abstractmethod
    def extract(self, filename):
        """ This will implement the actual extractor

        :param filename: is the filename (path+filename) to be processed
        :returns: a dictionary with the output of the extraction
        """
        raise NotImplementedError()

    @abstractmethod
    def mapping(self):
        """ This should return the Elastic type mapping related to result
        produced by extract

        :returns: a dictionary for the Elastic type mapping
        """
        raise NotImplementedError()
