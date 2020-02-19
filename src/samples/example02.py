# -*- coding: utf-8 -*-
"""
Created by Pierluigi on 2020-02-04
project: elasticizefiles
"""
import logging

from elasticizefiles.engine import ElasticizeEngine
from elasticizefiles.extractors.metadata import ExtractExif

logging.getLogger('elasticsearch').setLevel(logging.ERROR)
logging.getLogger('hachoir').setLevel(logging.ERROR)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def main():
    """ This will search for all jpeg files in current folder and for each
    it will apply :class:`ExtractExif`. The results will be stored
    in the index `jpgindex` in the Elastic instance running on localhost.

    Note: this requires hachoir package.
    """
    rules = {
        'rule01': {
            'pattern': [
                '.*\.(?i)jpg$', '.*\.(?i)jpeg$',
            ],
            'extractor': [
                {'exif': ExtractExif()},
            ],
        }
    }

    ElasticizeEngine(path='.',
                     rules=rules,
                     elastic_hosts=['localhost:9200', ],
                     elastic_index='jpgindex',
                     elastic_doc_type='file',
                     index_drop_if_exists=True,).crawl_and_process()


if __name__ == '__main__':
    main()
