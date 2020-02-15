# -*- coding: utf-8 -*-
"""
Created by Pierluigi on 2020-01-29
project: elasticizefiles
"""
from elasticizefiles.utils.files import explore_path
from elasticizefiles.utils.files import filestat


def test_explore_path():
    py_files = []
    for d, f in explore_path('.', recursive=False):
        if f.endswith('.py'):
            py_files.append(f)
    assert len(py_files) == 1, f'{len(py_files)} != 1'
    assert py_files[0] == 'setup.py', f'{py_files[0]} != setup.py'


def test_filestat():
    for k, _ in filestat('setup.py').items():
        assert k.startswith('st_'), f'{k} does not start with `st_`'
