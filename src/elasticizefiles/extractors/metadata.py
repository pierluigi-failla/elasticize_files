# -*- coding: utf-8 -*-
"""
Created by Pierluigi on 2020-02-04
project: elasticizefiles
"""
import logging
from elasticizefiles.base import Extractor


class ExtractExif(Extractor):

    def __init__(self):
        Extractor.__init__(self)

    def extract(self, filename):
        try:
            from hachoir.metadata import extractMetadata
            from hachoir.parser import createParser
        except Exception as e:
            raise Exception('module `hachoir` is not installed, try `pip install -U hachoir`')

        metadata = {}
        try:
            parser = createParser(filename)
            metadata = extractMetadata(parser).exportDictionary()['Metadata']
        except Exception as e:
            logging.warning(f'exception extracting metadata from {filename}. {e}')
        return metadata
