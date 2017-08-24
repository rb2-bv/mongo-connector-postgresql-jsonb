import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json
import logging

def upsert(cursor, table, doc_id, doc):
    cmd = sql.SQL('insert into {} (id, jdoc) values (%s, %s)').format(table)

    try:
        with cursor as c:
            return c.execute(cmd, (doc_id, Json(doc)))
    except Exception as e:
        log.error("Impossible to upsert %s to %s \n %s", doc, namespace, traceback.format_exc())

def update(cursor, table, doc_id, doc):
    return upsert(cursor, table, doc_id, doc)

def delete(cursor, table, doc_id):
    cmd = sql.SQL('delete from {} where id = %s').format(table)

    try:
        with cursor as c:
            return c.execute(cmd, doc_id)
    except Exception as e:
        log.error("Impossible to delete doc %s from %s \n %s", doc_id, namespace, traceback.format_exc())
