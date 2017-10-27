import logging
import sql

def _id_from_doc(doc):
    return doc['_id'] # TODO support object ID as well

def _table_from_namespace(namespace):
    db, collection = namespace.split('.', 1)
    return collection # TODO consider clashes if collection name shared across dbs

def upsert(cursor, namespace, doc):
    doc_id = _id_from_doc(doc)
    table = _table_from_namespace(namespace)
    return sql.upsert(cursor, table, doc_id, doc)

def update(cursor, namespace, doc_id, update_spec):
    pass

def delete(cursor, namespace, doc_id):
    table = _table_from_namespace(namespace)
    return sql.delete(cursor, table, doc_id)
