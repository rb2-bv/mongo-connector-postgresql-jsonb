import logging
import sql

log = logging.getLogger("ops")

def _id_from_doc(doc):
    return doc['_id'] # TODO support object ID as well

def _table_from_namespace(namespace):
    db, collection = namespace.split('.', 1)
    return collection # TODO consider clashes if collection name shared across dbs

def upsert(cursor, namespace, doc):
    doc_id = _id_from_doc(doc)
    table = _table_from_namespace(namespace)
    return sql.upsert(cursor, table, doc_id, doc)

def update(cursor, document_id, update_spec, namespace):
    update_jobject = update_spec_to_jobject(update_spec)
    return sql.update(cursor, _table_from_namespace(namespace), document_id, update_jobject)

def delete(cursor, namespace, doc_id):
    table = _table_from_namespace(namespace)
    return sql.delete(cursor, table, doc_id)

def update_spec_to_jobject(update_spec):
    mongo_update = update_spec.get("$set")
    sanitized_update = {}
    for key in mongo_update.keys():
        elements = key.split('.')
        elements.reverse()
        if len(elements) > 1:
            nested_update = {}
            for element in elements:
                if not nested_update:
                    nested_update = {element: mongo_update.get(key)}
                else:
                    nested_update = {element: nested_update}
            sanitized_update.update(nested_update)
        else:
            sanitized_update.update({key: mongo_update.get(key)})
    log.debug("Converted mongo update spec {} to dict {}".format(update_spec, sanitized_update))
    return sanitized_update