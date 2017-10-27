import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json
import json
import logging
import traceback
from datetime import date, datetime

log = logging.getLogger("psycopg2")

def upsert(cursor, table, doc_id, doc):
    cmd = sql.SQL("insert into {} (id, jdoc) values (%s, %s)").format(sql.Identifier(table))
    try:
        with cursor as c:
            return c.execute(cmd, (doc_id, Json(doc, dumps=dumps_json)))
    except Exception as e:
        log.error("Impossible to upsert %s to %s \n %s", doc, table, traceback.format_exc())

def update(cursor, table, doc_id, doc):
    return upsert(cursor, table, doc_id, doc)

def delete(cursor, table, doc_id):
    cmd = sql.SQL('delete from {} where id = %s').format(table)
    try:
        with cursor as c:
            return c.execute(cmd, doc_id)
    except Exception as e:
        log.error("Impossible to delete doc %s from %s \n %s", doc_id, table, traceback.format_exc())


def dumps_json(obj):
    return json.dumps(obj, default=custom_serializer)

def custom_serializer(obj):
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError ("%s is not JSON serializable" % type(obj))