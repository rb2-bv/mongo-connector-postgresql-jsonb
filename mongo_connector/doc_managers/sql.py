# -*- coding: utf-8 -*-

import json
import logging
import traceback
from datetime import date, datetime

from psycopg2 import sql
from psycopg2.extras import Json
from bson.objectid import ObjectId
from itertools import islice

log = logging.getLogger(__name__)


def _id_from_doc(doc):
    try:
        id = doc['_id']  # TODO support object ID as well
        return str(id)
    except KeyError:
        raise ValueError('Document did not contain an `_id` key. \n {}'.format(doc))


def custom_serializer(obj):
    try:
        if isinstance(obj, (date, datetime)):
            return obj.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        elif isinstance(obj, ObjectId):
            return str(obj)
        raise TypeError(u"%s is not JSON serializable" % type(obj))
    except Exception as e:
        log.error(u"Failed to serialize %s", obj, traceback.format_exc())
        raise e

def dumps_json(obj):
    return json.dumps(obj, default=custom_serializer)


def default_marshaller(obj):
    return Json(obj, dumps=dumps_json)


def split_every(n, iterable):
    # https://stackoverflow.com/questions/1915170
    i = iter(iterable)
    piece = list(islice(i, n))
    while piece:
        yield piece
        piece = list(islice(i, n))


def bulk_upsert(client, table, docs, marshaller=default_marshaller):
    for chunk in split_every(250, docs):
        try:
            inserts = []
            with client.cursor() as c:
                for doc in chunk:
                    try:
                        doc_id = _id_from_doc(doc)
                        marshalled_doc = marshaller(doc)
                        inserts.append(c.mogrify("(%s, %s)", (doc_id, marshalled_doc)).decode())
                    except Exception as e:
                        log.error(u"Failed to marshall %s, document will be discarded", doc, traceback.format_exc())
                log.info("Bulk upserting {} documents to {}".format(len(inserts), table))
                insert_string = ','.join(inserts)
                cmd = sql.SQL(
                    "insert into {} (id, jdoc) values {} on conflict (id) do update set jdoc = excluded.jdoc"
                ).format(sql.Identifier(table), sql.SQL(insert_string))
                c.execute(cmd)
        except Exception as e:
            log.error(u"Impossible to bulk upsert %s documents to %s \n %s", len(docs), table, traceback.format_exc())


def upsert(cursor, table, doc_id, doc, marshaller=default_marshaller):
    cmd = sql.SQL(
        "insert into {} (id, jdoc) values (%s, %s) on conflict (id) do update set jdoc = %s"
    ).format(sql.Identifier(table))
    try:
        with cursor as c:
            return c.execute(cmd, (doc_id, marshaller(doc), marshaller(doc)))
    except Exception as e:
        log.error(u"Impossible to upsert %s to %s \n %s", doc, table, traceback.format_exc())


def update(cursor, table, document_id, update_path, new_value, marshaller=default_marshaller):
    cmd = sql.SQL("update {} set jdoc=jsonb_set(jdoc, %s, %s::jsonb, true) where id = %s").format(sql.Identifier(table))
    try:
        return cursor.execute(cmd, (update_path, marshaller(new_value), document_id))
    except Exception as e:
        log.error(u"Failed to update %s with path: %s value: %s \n %s", document_id, update_path, new_value, traceback.format_exc())


def remove_keys(cursor, table, document_id, keys):
    cmd = sql.SQL("update {} set jdoc=(jdoc #- %s) where id = %s").format(sql.Identifier(table))
    try:
        with cursor as c:
            for key in keys:
                json_path = '{' + key.replace('.', ',') + '}'
                log.debug(u"Removing field at path {} from {}".format(json_path, document_id))
                c.execute(cmd, (json_path, document_id))
    except Exception as e:
        log.error(u"Failed to remove %s from %s \n %s", keys, document_id, traceback.format_exc())


def delete(cursor, table, doc_id):
    cmd = sql.SQL('delete from {} where id = %s').format(sql.Identifier(table))
    try:
        with cursor as c:
            return c.execute(cmd, (doc_id,))
    except Exception as e:
        log.error(u"Impossible to delete doc %s from %s \n %s", doc_id, table, traceback.format_exc())
