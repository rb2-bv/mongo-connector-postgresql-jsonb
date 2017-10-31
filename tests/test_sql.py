# -*- coding: utf-8 -*-

from unittest import TestCase, main
import psycopg2.sql as psql
from mongo_connector.doc_managers import sql
from mock import MagicMock, Mock
from datetime import datetime
from psycopg2.extras import Json as PsyCopJson


class TestSql(TestCase):
    @staticmethod
    def identity_marshaller(obj):
        return obj

    @staticmethod
    def mock_cursor():
        cursor_wrapper = MagicMock()
        cursor_mock = MagicMock()
        cursor_wrapper.__enter__ = Mock(return_value=cursor_mock)
        cursor_mock.execute.return_value = [1]
        return cursor_wrapper, cursor_mock

    def test_upsert(self):
        cursor_wrapper, cursor_mock = self.mock_cursor()
        document = {'foo': 'bar', "box": {'life': 42}}
        expected_sql = \
            psql.Composed([
                psql.SQL('insert into '),
                psql.Identifier('users'),
                psql.SQL(' (id, jdoc) values (%s, %s) on conflict (id) do update set jdoc = %s')]
            )
        sql.upsert(cursor_wrapper, 'users', '1234', document, self.identity_marshaller)
        cursor_mock.execute.assert_called_with(expected_sql, ('1234', document, document))

    def test_remove_keys(self):
        cursor_wrapper, cursor_mock = self.mock_cursor()
        sql.remove_keys(cursor_wrapper, 'users', 'foo-id', ['toplevelKey'])
        expected_sql = psql.Composed(
            [psql.SQL('update '), psql.Identifier('users'), psql.SQL(' set jdoc=(jdoc #- %s) where id = %s')]
        )
        cursor_mock.execute.assert_called_with(expected_sql, ('{toplevelKey}', 'foo-id'))

    def test_remove_keys_nested(self):
        cursor_wrapper, cursor_mock = self.mock_cursor()
        sql.remove_keys(cursor_wrapper, 'users', 'foo-id', ['nested.key'])
        expected_sql = psql.Composed(
            [psql.SQL('update '), psql.Identifier('users'), psql.SQL(' set jdoc=(jdoc #- %s) where id = %s')]
        )
        cursor_mock.execute.assert_called_with(expected_sql, ('{nested,key}', 'foo-id'))

    def test_delete(self):
        cursor_wrapper, cursor_mock = self.mock_cursor()
        sql.delete(cursor_wrapper, 'users', '1234')
        expected_sql = psql.Composed(
            [psql.SQL('delete from '), psql.Identifier('users'), psql.SQL(' where id = %s')]
        )
        cursor_mock.execute.assert_called_with(expected_sql, ('1234',))

    def test_default_marshaller(self):
        document = {'foo': 'bar', "box": {'life': datetime.now()}}
        json = sql.default_marshaller(document)
        self.assertEqual(json.getquoted(), PsyCopJson(document, sql.dumps_json).getquoted())

    def test_update(self):
        cursor_wrapper, cursor_mock = self.mock_cursor()
        document = {'foo': 'bar', "box": {'life': 42}}
        expected_sql = psql.Composed(
            [psql.SQL('update '), psql.Identifier('users'), psql.SQL(' set jdoc=(jdoc||%s) where id = %s')]
        )
        sql.update(cursor_wrapper, 'users', '1234', document, self.identity_marshaller)
        cursor_mock.execute.assert_called_with(expected_sql, (document, '1234'))


if __name__ == '__main__':
    main()
