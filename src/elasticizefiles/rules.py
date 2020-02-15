# -*- coding: utf-8 -*-
"""
Created by Pierluigi on 2020-02-01
project: elasticizefiles

This file contains the rules that will applied on the matching file. The 
dictionary is structured in the following way:

    1. every key is a rule
    2. every rule is composed by a `pattern` and an `extractor`
    2.1 `pattern` is a list of regex that will be applied on filenames (path 
        included) for the matching ones, the `extractor`s are applied
    2.2 `extractor` is a list of dictionaries: `{'function_name': class_instance}`
        `class_instance` in an instance of abstract class :class:`Extractor`.
        Each `extractor` must return a json-izable dictionary.
        
    Examples:

        from elasticizefiles.extractors.simple import ExtractFilename
        from elasticizefiles.extractors.simple import ExtractSha256

        rules = {
            'simple': {
                'pattern': ['.*\.txt$', ],
                'extractor': [
                    {'filename': ExtractFilename()},
                    {'sha256': ExtractSha256()}
                ],
            }
        }
        
    the above `simple` rule will be applied to every and each `.txt` file 
    extracting the filename and the file hash.
    
"""

from elasticizefiles.extractors.simple import ExtractFilename
from elasticizefiles.extractors.simple import ExtractSha256

rules = {
    'rule1': {
        'pattern': [
            '.*\.txt$',
        ],
        'extractor': [
            {'fn': ExtractFilename()},
            {'sha256': ExtractSha256()},
        ],
    }
}
