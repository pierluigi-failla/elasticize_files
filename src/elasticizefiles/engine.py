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

    def __init__(self, path, rules, elastic_hosts, elastic_index, elastic_doc_type):
        self._path = path
        ElasticizeEngine._check_rules(rules)
        self._rules = rules
        self._machine_info = get_machine_info()

        self._es = Elastic(elastic_hosts, elastic_index, elastic_doc_type)
        self._es.create_index(elastic_index, elastic_doc_type)

    def crawl_and_process(self, n_jobs=-1):
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
        r = {
            'file_id': file_id,
            'scan_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
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