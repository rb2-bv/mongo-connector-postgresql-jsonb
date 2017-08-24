import traceback
import logging

import psycopg2
from mongo_connector.doc_managers.doc_manager_base import DocManagerBase

import ops

from mongo_connector.doc_managers.sql import (
    upsert,
    update,
    delete
)

log = logging.getLogger(__name__)

class DocManager(DocManagerBase):
    """DocManager that connects to Postgres"""

    def __init__(self, url, unique_key='_id', auto_commit_interval=None, chunk_size=100, **kwargs):
        self.pg_client = psycopg2.connect(url)

    def stop(self):
        log.info('stop!')

    def upsert(self, doc, namespace, timestamp):
        log.info('upsert! with %s' % doc)
        return ops.upsert(self.pg_client.cursor(), namespace, doc)

    def bulk_upsert(self, docs, namespace, timestamp):
        log.info('bulk_upsert! with %s' % docs)

    def update(self, document_id, update_spec, namespace, timestamp):
        log.info('update! with %s' % document_id)
        return ops.upsert(self.pg_client.cursor(), namespace, doc)

    def remove(self, document_id, namespace, timestamp):
        log.info('remove! with %s' % document_id)
        return ops.delete(self.pg_client.cursor(), namespace, document_id)

    def search(self, start_ts, end_ts):
        log.info('search! with %s' % start_ts)
        pass

    def commit(self):
        log.info('commit!')
        pass

    def get_last_doc(self):
        log.info('get_last_doc!')
        pass

    def handle_command(self, doc, namespace, timestamp):
        log.info('handle_command! with %s' % doc)
        pass
