# -*- coding: utf-8 -*-

import logging

from . import sql
from bson.objectid import ObjectId
from itertools import islice

log = logging.getLogger(__name__)

def _id_from_doc(doc):
    try:
        id = doc['_id']  # TODO support object ID as well
        return str(id)
    except KeyError:
        raise ValueError('Document did not contain an `_id` key. \n {}'.format(doc))


def _table_from_namespace(namespace):
    try:
        db, collection = namespace.split('.', 1)
        return "mt_doc_" + collection.lower()  # TODO consider clashes if collection name shared across dbs
    except ValueError:
        raise ValueError('Namespaces must be of the form namespace.collection, got:{}'.format(namespace))


def bulk_upsert(client, docs, namespace, timestamp):
    try:
        table = _table_from_namespace(namespace)
        sql.bulk_upsert(client, table, docs)
    except Exception as e:
        raise e


def upsert(cursor, namespace, doc):
    doc_id = _id_from_doc(doc)
    table = _table_from_namespace(namespace)
    return sql.upsert(cursor, table, doc_id, doc)


def update(cursor, document_id, update_spec, namespace):
    table = _table_from_namespace(namespace)
    if update_spec.get("$set"):
        updates = update_to_path_and_value(update_spec)
        with cursor as c:
            for (path, value) in updates:
                log.debug(u'Updating path: {} with value: {}'.format(path, value))
                sql.update(c, table, document_id, path, value)
    if update_spec.get("$unset"):
        keys_to_unset = update_spec.get("$unset").keys()
        sql.remove_keys(cursor, table, document_id, keys_to_unset)


def delete(cursor, namespace, doc_id):
    table = _table_from_namespace(namespace)
    return sql.delete(cursor, table, doc_id)


def update_to_path_and_value(update_spec):
    set_fields = update_spec.get('$set')
    updates = []
    for key in set_fields.keys():
        updates.append(('{' + key.replace('.', ',') + '}', set_fields.get(key)))
    return updates
