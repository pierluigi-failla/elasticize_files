# -*- coding: utf-8 -*-
"""
Created by Pierluigi on 2020-02-02
project: elasticizefiles
"""
from elasticizefiles.base import Extractor
from elasticizefiles.utils.files import get_hash


class ExtractFilename(Extractor):

    def __init__(self):
        Extractor.__init__(self)

    def extract(self, filename):
        return {'filename': filename}

    def mapping(self):
        return {
            'filename': {
                'type': 'text',
                'fields': {
                    'keywords': {
                        'type': 'keyword',
                    }
                }
            }
        }


class ExtractSha256(Extractor):

    def __init__(self):
        Extractor.__init__(self)

    def extract(self, filename):
        return {'sha256': get_hash(filename, hash_type='sha256')}

    def mapping(self):
        return {
            'sha256': {
                'type': 'text',
            }
        }


class ExtractPythonFunction(Extractor):
    """ Extract function and class from python code. """

    def __init__(self):
        Extractor.__init__(self)

    def extract(self, filename):
        r = {
            'class': [],
            'function': [],
        }
        with open(filename, 'r') as f:
            c = 0
            for line in f.readlines():
                c += 1
                line = line.replace('\r', '').replace('\n', '').replace('\t',
                                                                        ' ').strip()
                if line.startswith('class'):
                    r['class'].append({'line': c, 'name': line})
                    continue
                if line.startswith('def'):
                    r['function'].append({'line': c, 'name': line})
                    continue
        return r

    def mapping(self):
        return {
            'class': {
                'properties': {
                    'line': {'type': 'long', },
                    'name': {
                        'type': 'text',
                        'fields': {
                            'keywords': {'type': 'keyword', }
                        }
                    }
                }
            },
            'function': {
                'properties': {
                    'line': {'type': 'long', },
                    'name': {
                        'type': 'text',
                        'fields': {
                            'keywords': {'type': 'keyword', }
                        }
                    }
                }
            },
        }
