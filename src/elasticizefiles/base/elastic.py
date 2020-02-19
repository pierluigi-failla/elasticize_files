# -*- coding: utf-8 -*-
"""
Created by Pierluigi on 2020-02-03
project: elasticizefiles
"""

import logging
import math

from elasticsearch import Elasticsearch
from elasticsearch.connection import RequestsHttpConnection


class Elastic(object):
    """ A wrapper to ElasticSearch

    OR is spelled should
    AND is spelled must
    NOR is spelled should_not

    :param hosts: list of nodes {host}:{port}
    :param index: index name
    :param doc_type: document type
    """

    def __init__(self, hosts, index, doc_type):
        self._hosts = hosts
        self._index = index
        self._doc_type = doc_type

    def create_index(self, index_name, doc_type, create_if_not_exists=True,
                     drop_if_exists=False, config=None, mapping=None,
                     alias_name=None):
        """ Simplified index creation

        :param index_name: an index name
        :param doc_type: the elastic doc_type name
        :param create_if_not_exists: if True create the index if not exists
        :param drop_if_exists: if True drop the existing index
        :param config: additional config params to create the index
        :param mapping: the data type mapping for the current index
        :param alias_name: an alias name for this index
        """
        es = Elasticsearch(hosts=self._hosts, http_compress=True,
                           connection_class=RequestsHttpConnection,
                           timeout=30)
        if drop_if_exists:
            es.indices.delete(index=index_name)
            logging.debug(f'index: {index_name} dropped')
        if es.indices.exists(index_name):
            logging.debug(f'index: {index_name} exists')
        else:
            logging.debug(f'index: {index_name} does not exists')
            if create_if_not_exists:
                es.indices.create(index_name, body=config)
                if mapping is not None:
                    es.indices.put_mapping(index=index_name,
                                           doc_type=doc_type,
                                           body=mapping,
                                           include_type_name=True, )
                if alias_name is not None:
                    es.indices.put_alias(index=index_name, name=alias_name)
        self._index = index_name
        self._doc_type = doc_type
        return self

    def iterate_data(self, query, page_size=100, raw=False):
        """ A generator to collect data from ES matching `query`

        :param query: ES query dictionary
        :param page_size: used for pagination (do not touch if you do not know)
        :param raw: whether return elastic metadata too or not
        :return: a list of documents
        """
        logging.debug(f'iterate_data from {self._index}')
        try:
            for page in self._scroll(query, page_size=page_size):
                docs = page['hits']['hits']
                if len(docs) == 0:
                    break
                for doc in docs:
                    if raw is False:
                        yield doc['_source']
                    else:
                        yield doc
        except Exception as e:
            logging.warning(f'exception: {e}')
            raise e

    def get_data(self, query, limit=1000, page_size=100, raw=False):
        """ Collect data from ES matching `query`

        :param query: ES query dictionary
        :param limit: max number of docs to be returned
        :param page_size: used for pagination (do not touch if you do not know)
        :param raw: whether return elastic metadata too or not
        :return: a list of documents
        """
        logging.debug(f'get_data from {self._index}')
        _data = []
        try:
            for page in self._scroll(query, page_size=page_size):
                docs = page['hits']['hits']
                if len(docs) == 0:
                    break
                if raw is False:
                    for doc in docs:
                        _data.append(doc['_source'])
                else:
                    _data.extend(docs)
                logging.debug(f'collected {len(_data)} docs')
                if len(_data) >= limit:
                    break
        except Exception as e:
            logging.warning(f'[es] exception: {e}')
            raise e
        return _data[:limit]

    def _scroll(self, query, page_size=100, scroll='5m'):
        """ Internal helper to ES scroll
        """
        es = Elasticsearch(hosts=self._hosts, http_compress=True,
                           connection_class=RequestsHttpConnection,
                           timeout=30)
        page = es.search(index=self._index, doc_type=self._doc_type,
                         scroll=scroll, size=page_size, body=query)
        sid = page['_scroll_id']
        scroll_size = page['hits']['total']['value']
        page_counter = 0
        logging.debug(f'total items : {scroll_size}')
        logging.debug(f'total pages : {math.ceil(scroll_size / page_size)}')
        # Start scrolling
        while scroll_size > 0:
            # Get the number of results that we returned in the last scroll
            scroll_size = len(page['hits']['hits'])
            if scroll_size > 0:
                logging.debug((f'> scrolling page {page_counter} : '
                               f'{scroll_size} items'))
                yield page
            # get next page
            page = es.scroll(scroll_id=sid, scroll=scroll)
            page_counter += 1
            # Update the scroll ID
            sid = page['_scroll_id']

    def search(self, query, **kwargs):
        """ES search wrapper"""
        logging.debug(f'search in {self._index}')
        es = Elasticsearch(hosts=self._hosts, http_compress=True,
                           connection_class=RequestsHttpConnection,
                           timeout=30)
        try:
            res = es.search(index=self._index, doc_type=self._doc_type,
                            body=query, **kwargs)
        except Exception as e:
            logging.warning(f'exception: {e}')
            raise e
        return res

    def store(self, data):
        """ES store wrapper"""
        logging.debug(f'store in {self._index}')
        es = Elasticsearch(hosts=self._hosts, http_compress=True,
                           connection_class=RequestsHttpConnection,
                           timeout=30)
        try:
            r = es.index(index=self._index, doc_type=self._doc_type,
                         body=data)
            return r['_id']
        except Exception as e:
            logging.warning(f'exception: {e}')
            raise e

    def update(self, id, data, upsert=True):
        """ES update wrapper"""
        logging.debug(f'update in {self._index}')
        es = Elasticsearch(hosts=self._hosts, http_compress=True,
                           connection_class=RequestsHttpConnection,
                           timeout=30)
        try:
            r = es.update(index=self._index, doc_type=self._doc_type,
                          id=id, body={'doc': data, 'doc_as_upsert': upsert})
        except Exception as e:
            logging.warning(f'exception: {e}')
            raise e
