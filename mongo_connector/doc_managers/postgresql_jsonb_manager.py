# -*- coding: utf-8 -*-

import logging

import psycopg2
from mongo_connector.doc_managers.doc_manager_base import DocManagerBase
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

from . import ops

log = logging.getLogger(__name__)


class DocManager(DocManagerBase):
    """DocManager that connects to Postgres"""

    def insert_file(self, f, namespace, timestamp):
        pass

    def __init__(self, url, unique_key='_id', auto_commit_interval=None, chunk_size=100, **kwargs):
        self.pg_client = psycopg2.connect(url)
        self.pg_client.autocommit = True
        self.pool = ThreadPool()

    def stop(self):
        log.info('Stopping')
        self.pool.join()
        self.pg_client.commit()
        self.pg_client.close()

    def upsert(self, doc, namespace, timestamp):
        log.debug('upsert with %s' % doc)
        return self.pool.apply_async(ops.upsert, (self.pg_client.cursor(), namespace, doc))

    def bulk_upsert(self, docs, namespace, timestamp):
        return self.pool.apply_async(ops.bulk_upsert, (self.pg_client.cursor(), docs, namespace, timestamp))

    def update(self, document_id, update_spec, namespace, timestamp):
        log.debug('update! with id: {} update_spec: {}'.format(document_id, update_spec))
        return self.pool.apply_async(ops.update, (self.pg_client.cursor(), document_id, update_spec, namespace))

    def remove(self, document_id, namespace, timestamp):
        log.debug('remove! with %s' % document_id)
        return self.pool.apply_async(ops.delete, (self.pg_client.cursor(), namespace, document_id))

    def search(self, start_ts, end_ts):
        pass

    def commit(self):
        log.info('Commiting')
        self.pg_client.commit()

    def get_last_doc(self):
        pass

    def handle_command(self, doc, namespace, timestamp):
        pass
