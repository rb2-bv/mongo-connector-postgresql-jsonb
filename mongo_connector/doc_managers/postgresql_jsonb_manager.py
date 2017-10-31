# -*- coding: utf-8 -*-

import logging

import psycopg2
from mongo_connector.doc_managers.doc_manager_base import DocManagerBase

from . import ops

log = logging.getLogger(__name__)


class DocManager(DocManagerBase):
    """DocManager that connects to Postgres"""

    def insert_file(self, f, namespace, timestamp):
        pass

    def __init__(self, url, unique_key='_id', auto_commit_interval=None, chunk_size=100, **kwargs):
        self.pg_client = psycopg2.connect(url)
        self.pg_client.autocommit = True

    def stop(self):
        log.info('Stopping')
        self.pg_client.commit()
        self.pg_client.close()

    def upsert(self, doc, namespace, timestamp):
        log.debug('upsert with %s' % doc)
        return ops.upsert(self.pg_client.cursor(), namespace, doc)

    def bulk_upsert(self, docs, namespace, timestamp):
        for doc in docs:
            self.upsert(doc, namespace, timestamp)

    def update(self, document_id, update_spec, namespace, timestamp):
        log.debug('update! with %s' % document_id)
        return ops.update(self.pg_client.cursor(), document_id, update_spec, namespace)

    def remove(self, document_id, namespace, timestamp):
        log.debug('remove! with %s' % document_id)
        return ops.delete(self.pg_client.cursor(), namespace, document_id)

    def search(self, start_ts, end_ts):
        pass

    def commit(self):
        log.info('Commiting')
        self.pg_client.commit()

    def get_last_doc(self):
        pass

    def handle_command(self, doc, namespace, timestamp):
        pass
