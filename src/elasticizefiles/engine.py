# -*- coding: utf-8 -*-
"""
Created by Pierluigi on 2020-02-01
project: elasticizefiles
"""
import logging
import os
import re
from datetime import datetime
from hashlib import sha256
from multiprocessing import cpu_count
from time import time

from joblib import Parallel
from joblib import delayed

from elasticizefiles.base import Elastic
from elasticizefiles.utils.files import explore_path
from elasticizefiles.utils.files import filestat
from elasticizefiles.utils.files import get_hash
from elasticizefiles.utils.files import get_machine_info


class ElasticizeEngine(object):
    """ The main engine to scan, process and store files

    :param path: the path to be scanned
    :param rules: the rules to be applied
    :param elastic_hosts: list of nodes ['{host}:{port}', ...]
    :param elastic_index: an index name
    :param elastic_doc_type: the elastic doc_type name
    :param index_create_if_not_exists: if True create the index if not exists
    :param index_drop_if_exists: if True drop the existing index
    :param index_config: additional config params to create the index
    :param index_mapping: the data type mapping for the current index
    :param index_alias_name: an alias name for this index
    """

    def __init__(self, path, rules, elastic_hosts, elastic_index, elastic_doc_type,
                 index_create_if_not_exists=True, index_drop_if_exists=False,
                 index_config=None, index_mapping=None, index_alias_name=None):

        self._path = path
        ElasticizeEngine._check_rules(rules)
        self._rules = rules
        self._machine_info = get_machine_info()

        if index_mapping is None:
            index_mapping = ElasticizeEngine._build_mapping(rules)

        self._es = Elastic(elastic_hosts, elastic_index, elastic_doc_type)
        self._es.create_index(elastic_index, elastic_doc_type,
                              create_if_not_exists=index_create_if_not_exists,
                              drop_if_exists=index_drop_if_exists,
                              config=index_config, mapping=index_mapping,
                              alias_name=index_alias_name)

    def crawl_and_process(self, n_jobs=-1):
        """ Crawl files and apply extractor on them.

        :param n_jobs: number of parallel job, if -1 will be automatically set
                       to the number of cpus available
        """
        if n_jobs < 1:
            n_jobs = cpu_count()
        tot = 0
        buffer = []
        tik = time()
        for dirname, filename in explore_path(self._path):
            full_filename = os.path.abspath(os.path.join(dirname, filename)).replace('\\', '/')
            exts = []
            for rule_name, rule in self._rules.items():
                for pattern in rule['pattern']:
                    if re.match(pattern, full_filename):
                        logging.debug(f'matched: {full_filename}')
                        for extractor in rule['extractor']:
                            for n, e in extractor.items():
                                exts.append({f'{rule_name}.{n}': e})
            if len(exts) > 0:
                buffer.append((full_filename, exts))
            if len(buffer) >= 5 * n_jobs:
                Parallel(n_jobs=n_jobs, verbose=1, backend='threading')(map(delayed(self._applier), buffer))
                tot += len(buffer)
                logging.info(f'completed: {tot} files ({(time() - tik) / tot:.2f}s per file)')
                buffer = []
        if len(buffer) > 0:
            Parallel(n_jobs=n_jobs, verbose=1, backend='threading')(map(delayed(self._applier), buffer))
            tot += len(buffer)
            logging.info(f'completed: {tot} files ({(time() - tik) / tot:.2f}s per file)')
        logging.info(f'completed {tot} in {(time() - tik):.2f}s')

    def _applier(self, args):
        filename, exts = args
        logging.info(f'processing: {filename}')
        sha = get_hash(filename, hash_type='sha256')
        file_id = sha256((self._machine_info['mac_address'] + sha + filename).encode()).hexdigest()
        now = datetime.now()
        r = {
            'file_id': file_id,
            'scan_datetime': now.strftime('%Y-%m-%d %H:%M:%S'),
            'scan_timestamp': now.timestamp() * 1000,
            'sha256': sha,
            'filename': filename,
            'file_stats': filestat(filename),
            'machine_info': self._machine_info,
        }
        for fun in exts:
            for name, obj in fun.items():
                r[name] = obj.extract(filename)
        self._es.update(id=r['file_id'], data=r)

    @staticmethod
    def _check_rules(rules):
        """ Check the `rules` to ensure patterns and extractors.

        :raise: exceptions if `patterns` are not regex or `extractors` are not
        derived from :class:`Extractor`.
        """
        c = {
            'rules': 0,
            'patterns': 0,
            'extractors': 0,
        }
        for _, r in rules.items():
            c['rules'] += 1
            for p in r['pattern']:
                c['patterns'] += 1
                re.compile(p)
            for e in r['extractor']:
                c['extractors'] += 1
                for _, obj in e.items():
                    if not (hasattr(obj, 'extract') and callable(obj.extract)):
                        logging.warning(f'{type(obj)} does not implement extract seems not to be an Extractor')
                        raise Exception(f'{type(obj)} seems not to be an Extractor')
        for k, v in c.items():
            logging.info(f'checked: {v} {k}')

    @staticmethod
    def _build_mapping(rules):
        mapping = {
            'file_id': {'type': 'text', },
            'scan_datetime': {
                'type': 'date',
                'format': 'yyyy-MM-dd HH:mm:ss',
            },
            'scan_timestamp': {'type': 'float'},
            'sha256': {'type': 'text', },
            'filename': {
                'type': 'text',
                'fields': {
                    'keywords': {
                        'type': 'keyword',
                    }
                }
            },
            'file_stats': {
                'properties': {
                    'st_atime': {'type': 'float'},
                    'st_atime_ns': {'type': 'long'},
                    'st_ctime': {'type': 'float'},
                    'st_ctime_ns': {'type': 'long'},
                    'st_dev': {'type': 'long'},
                    'st_file_attributes': {'type': 'long'},
                    'st_gid': {'type': 'long'},
                    'st_ino': {'type': 'long'},
                    'st_mode': {'type': 'long'},
                    'st_mtime': {'type': 'float'},
                    'st_mtime_ns': {'type': 'long'},
                    'st_nlink': {'type': 'long'},
                    'st_size': {'type': 'long'},
                    'st_uid': {'type': 'long'},
                },
            },
            'machine_info': {
                'properties': {
                    'platform': {'type': 'text'},
                    'hostname': {'type': 'text'},
                    'ip_address': {'type': 'text'},
                    'mac_address': {'type': 'text'},
                }
            },
        }
        for rule_name, rule in rules.items():
            for _ in rule['pattern']:
                for extractor in rule['extractor']:
                    for n, e in extractor.items():
                        _map = e.mapping()
                        if _map:
                            mapping[f'{rule_name}.{n}'] = {'properties': _map}
        return {'properties': mapping}
