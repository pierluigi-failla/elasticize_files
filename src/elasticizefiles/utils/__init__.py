# -*- coding: utf-8 -*-
"""
Created by Pierluigi on 2020-01-29
project: elasticizefiles
"""
from re import sub


def to_snake(name):
    """Convert :param name: to snake case"""
    s1 = sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
