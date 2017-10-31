# -*- coding: utf-8 -*-

from behave import *
from pymongo import MongoClient
from psycopg2 import sql
from datetime import datetime
from hamcrest import assert_that, equal_to

import time
import psycopg2
import subprocess
import json


def eventually(f, step_time_seconds, max_trys=20, current_try=0):
    try:
        f()
    except Exception as e:
        if current_try <= max_trys:
            time.sleep(step_time_seconds)
            eventually(f, max_trys, step_time_seconds, current_try + 1)
        else:
            raise e


def find_by_id_and_assert_equal(context, id, expected_obj):
    with context.pg_client.cursor() as cursor:
        cursor.execute(sql.SQL('select jdoc from collection1 where id=%s'), (id,))
        json_dict = cursor.fetchone()[0]
        assert_that(json_dict, equal_to(expected_obj))


@given("mongodb and postgres are running")
def step_impl(context):
    context.mongo_client = MongoClient(context.mongo_url)
    context.mongo_db = context.mongo_client.get_database('database')
    context.mongo_col = context.mongo_db['collection1']
    pg_client = psycopg2.connect(context.postgres_url)
    pg_client.autocommit = True
    context.pg_client = pg_client
    with pg_client.cursor() as c:
        c.execute(sql.SQL('select 1'))


@given(u'the jsonb connector is running targeting the mongo collection \'database.collection1\'')
def step_impl(context):
    cmd = 'PYTHONPATH={} mongo-connector -m {} -t {} -n \'database.collection1\' -d postgresql_jsonb_manager -v'.format(
        context.project_root, context.mongo_url, context.postgres_url
    )
    print('Starting connector with command {}'.format(cmd))
    context.mongo_connector = subprocess.Popen(
        cmd,
        shell=True,
        cwd=context.project_root
    )


@step("there is an empty postgres table named 'collection1'")
def step_impl(context):
    with context.pg_client.cursor() as c:
        drop_result = c.execute(sql.SQL('DROP TABLE IF EXISTS collection1;'))
        create_result = c.execute(
            sql.SQL('CREATE TABLE collection1(id VARCHAR NOT NULL PRIMARY KEY, jdoc jsonb NOT NULL);'))


@when("a document is inserted into the mongo collection")
def step_impl(context):
    document = {
        '_id': 'test-insert',
        'singleValue': 42,
        'boxedValue': {
            'box': 'foobar'
        }
    }
    context.expected_document = document
    context.expected_insert_id = context.mongo_col.insert_one(document).inserted_id


@then("the document is inserted into to the collection1 table in postgres")
def step_impl(context):
    def document_is_copied_to_postgres():
        find_by_id_and_assert_equal(context, context.expected_insert_id, context.expected_document)

    eventually(document_is_copied_to_postgres, 0.5)


@step("there is an empty mongo collection 'database.collection1'")
def step_impl(context):
    context.mongo_db.collection1.drop()


@given("the jsonb connector is running")
def step_impl(context):
    assert context.mongo_connector.poll() is None


@given("a document exists in mongodb and postgres")
def step_impl(context):
    document = {
        '_id': 'test-document',
        'singleValue': 42
    }
    context.document_id_that_exists = context.mongo_col.insert_one(document).inserted_id
    context.document = document
    def document_is_copied_to_postgres():
        find_by_id_and_assert_equal(context, context.document_id_that_exists, document)

    eventually(document_is_copied_to_postgres, 0.5)


@when("the document is deleted from the mongo collection")
def step_impl(context):
    context.mongo_col.delete_one({'_id': context.document_id_that_exists})


@then("the document is deleted from the collection1 table in postgres")
def step_impl(context):
    def document_deleted_from_postgres():
        with context.pg_client.cursor() as cursor:
            cursor.execute(sql.SQL('select count(*) from collection1 where id=%s'), (context.document_id_that_exists,))
            count = cursor.fetchone()[0]
            assert_that(count, equal_to(0))

    eventually(document_deleted_from_postgres, step_time_seconds=0.5)


@when("a field in the document is updated in mongo")
def step_impl(context):
    context.mongo_col.update({
        '_id': context.document_id_that_exists
    }, {
        '$set': {
            'singleValue': 43
        }
    })


@then("the document in postgres reflects the update")
def step_impl(context):
    expected_doc = {
        '_id': context.document_id_that_exists,
        'singleValue': 43
    }

    def check_updated():
        find_by_id_and_assert_equal(context, context.document_id_that_exists, expected_doc)

    eventually(check_updated, 0.5)


@when("a field in the document is unset in mongo")
def step_impl(context):
    context.mongo_col.update({
        '_id': context.document_id_that_exists
    }, {
        '$unset': {
            'singleValue': 1
        }
    })


@then("the document in postgres reflects the unset")
def step_impl(context):
    expected_doc = {
        '_id': context.document_id_that_exists
    }

    def check_updated():
        find_by_id_and_assert_equal(context, context.document_id_that_exists, expected_doc)

    eventually(check_updated, 0.5)


@given("a document exists with a nested value in mongodb and postgres")
def step_impl(context):
    document = {
        '_id': 'test-document',
        'nestedValue': {
            'a': 1,
            'b': 2
        }
    }
    context.document_id_that_exists = context.mongo_col.insert_one(document).inserted_id
    context.document = document

    def document_is_copied_to_postgres():
        find_by_id_and_assert_equal(context, context.document_id_that_exists, document)

    eventually(document_is_copied_to_postgres, 0.5, max_trys=10)


@when("a nested field in the document is unset in mongo")
def step_impl(context):
    context.mongo_col.update({
        '_id': context.document_id_that_exists
    }, {
        '$unset': {
            'nestedValue.a': 1
        }
    })


@then("the nested field is removed in postgres")
def step_impl(context):
    expected_doc = {
        '_id': context.document_id_that_exists,
        'nestedValue': {
            'b': 2
        }
    }

    def check_updated():
        find_by_id_and_assert_equal(context, context.document_id_that_exists, expected_doc)

    eventually(check_updated, 0.5)


@when("a nested field in the document is updated in mongo")
def step_impl(context):
    context.mongo_col.update({
        '_id': context.document_id_that_exists
    }, {
        '$set': {
            'nestedValue.b': 42
        }
    })


@then("the nested field is updated in postgres")
def step_impl(context):
    expected_doc = {
        '_id': context.document_id_that_exists,
        'nestedValue': {
            'a': 1,
            'b': 42
        }
    }

    def check_updated():
        find_by_id_and_assert_equal(context, context.document_id_that_exists, expected_doc)

    eventually(check_updated, 0.5)
