# -*- coding: utf-8 -*-

from unittest import TestCase, main
from mongo_connector.doc_managers import ops


class TestOps(TestCase):
    def test_set_fields_to_dict_not_nested(self):
        update_spec = {'$set': {'foo': 'bar', 'baz': 'box'}}
        actual = ops.set_fields_to_dict(update_spec)
        expected = {'foo': 'bar', 'baz': 'box'}
        self.assertEqual(actual, expected)

    def test_set_fields_to_dict_nested(self):
        update_spec = {'$set': {'foo.bar': 'baz', 'foo.box': 'baz'}}
        actual = ops.set_fields_to_dict(update_spec)
        expected = {'foo': {'bar': 'baz', 'box': 'baz'}}
        self.assertEqual(actual, expected)

    def test_deep_merge(self):
        a = {'a': {'1': 1}}
        b = {'a': {'1': 2, '2': 2, 'c': {'d': 4}}}
        actual = ops.deep_merge(b, a)
        expected = {'a': {'1': 2, '2': 2, 'c': {'d': 4}}}
        self.assertEqual(actual, expected)

    def test_table_from_namespace(self):
        actual = ops._table_from_namespace('table.collection')
        self.assertEqual(actual, 'collection')
        self.assertRaises(ValueError, ops._table_from_namespace, 'invalid')

    def test_id_from_doc(self):
        valid_doc = {'_id': '1234'}
        self.assertEqual(ops._id_from_doc(valid_doc), '1234')
        self.assertRaises(ValueError, ops._id_from_doc, {'id': '1234'})


if __name__ == '__main__':
    main()
