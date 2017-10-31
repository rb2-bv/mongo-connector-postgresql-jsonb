# -*- coding: utf-8 -*-

import json
import logging
import traceback
from datetime import date, datetime

from psycopg2 import sql
from psycopg2.extras import Json

log = logging.getLogger("psycopg2")


def custom_serializer(obj):
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError("%s is not JSON serializable" % type(obj))


def dumps_json(obj):
    return json.dumps(obj, default=custom_serializer)


def default_marshaller(obj):
    return Json(obj, dumps=dumps_json)


def upsert(cursor, table, doc_id, doc, marshaller=default_marshaller):
    cmd = sql.SQL(
        "insert into {} (id, jdoc) values (%s, %s) on conflict (id) do update set jdoc = %s"
    ).format(sql.Identifier(table))
    try:
        with cursor as c:
            return c.execute(cmd, (doc_id, marshaller(doc), marshaller(doc)))
    except Exception as e:
        log.error("Impossible to upsert %s to %s \n %s", doc, table, traceback.format_exc())


def update(cursor, table, document_id, update_path, new_value, marshaller=default_marshaller):
    cmd = sql.SQL("update {} set jdoc=jsonb_set(jdoc, %s, %s::jsonb, true) where id = %s").format(sql.Identifier(table))
    try:
        with cursor as c:
            return c.execute(cmd, (update_path, marshaller(new_value), document_id))
    except Exception as e:
        log.error("Failed to update %s with path: %s value: %s \n %s", document_id, update_path, new_value, traceback.format_exc())



def remove_keys(cursor, table, document_id, keys):
    cmd = sql.SQL("update {} set jdoc=(jdoc #- %s) where id = %s").format(sql.Identifier(table))
    try:
        with cursor as c:
            for key in keys:
                json_path = '{' + key.replace('.', ',') + '}'
                log.debug("Removing field at path {} from {}".format(json_path, document_id))
                c.execute(cmd, (json_path, document_id))
    except Exception as e:
        log.error("Failed to remove %s from %s \n %s", keys, document_id, traceback.format_exc())


def delete(cursor, table, doc_id):
    cmd = sql.SQL('delete from {} where id = %s').format(sql.Identifier(table))
    try:
        with cursor as c:
            return c.execute(cmd, (doc_id,))
    except Exception as e:
        log.error("Impossible to delete doc %s from %s \n %s", doc_id, table, traceback.format_exc())
