# -*- coding: utf-8 -*-
"""
Created by Pierluigi on 2020-02-03
project: elasticizefiles
"""
import logging

from elasticizefiles.engine import ElasticizeEngine
from elasticizefiles.extractors.simple import ExtractPythonFunction

logging.getLogger('elasticsearch').setLevel(logging.ERROR)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def main():
    """ This will search for all .py files in current folder and for each
    it will apply :class:`ExtractPythonFunction`. The results will be stored
    in the index `fileindex` in the Elastic instance running on localhost.
    """
    rules = {
        'rule01': {
            'pattern': [
                '.*\.(?i)py$',
            ],
            'extractor': [
                {'python_fun': ExtractPythonFunction()},
            ],
        }
    }

    ElasticizeEngine(path='.',
                     rules=rules,
                     elastic_hosts=['localhost:9200', ],
                     elastic_index='fileindex',
                     elastic_doc_type='file').crawl_and_process()


if __name__ == '__main__':
    main()
