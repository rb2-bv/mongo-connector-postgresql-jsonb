# -*- coding: utf-8 -*-

import logging
from . import sql

log = logging.getLogger("ops")


def _id_from_doc(doc):
    try:
        return doc['_id']  # TODO support object ID as well
    except KeyError:
        raise ValueError('Document did not contain an `_id` key. \n {}'.format(doc))


def _table_from_namespace(namespace):
    try:
        db, collection = namespace.split('.', 1)
        return collection  # TODO consider clashes if collection name shared across dbs
    except ValueError:
        raise ValueError('Namespaces must be of the form namespace.collection, got:{}'.format(namespace))


def upsert(cursor, namespace, doc):
    doc_id = _id_from_doc(doc)
    table = _table_from_namespace(namespace)
    return sql.upsert(cursor, table, doc_id, doc)


def update(cursor, document_id, update_spec, namespace):
    table = _table_from_namespace(namespace)
    if update_spec.get("$set"):
        update_jobject = set_fields_to_dict(update_spec)
        sql.update(cursor, table, document_id, update_jobject)
    if update_spec.get("$unset"):
        keys_to_unset = update_spec.get("$unset").keys()
        sql.remove_keys(cursor, table, document_id, keys_to_unset)


def delete(cursor, namespace, doc_id):
    table = _table_from_namespace(namespace)
    return sql.delete(cursor, table, doc_id)


def set_fields_to_dict(update_spec):
    sanitized_update = {}
    set_fields = update_spec.get("$set")
    for key in set_fields.keys():
        elements = key.split('.')
        elements.reverse()
        if len(elements) > 1:
            nested_update = {}
            for element in elements:
                if not nested_update:
                    nested_update = {element: set_fields.get(key)}
                else:
                    nested_update = {element: nested_update}
            sanitized_update = deep_merge(nested_update, sanitized_update)
        else:
            sanitized_update = deep_merge({key: set_fields.get(key)}, sanitized_update)
    log.debug("Converted mongo update spec {} to dict {}".format(update_spec, sanitized_update))
    return sanitized_update


def deep_merge(source, destination):
    for key, value in source.items():
        if isinstance(value, dict):
            node = destination.setdefault(key, {})
            deep_merge(value, node)
        else:
            destination[key] = value
    return destination
