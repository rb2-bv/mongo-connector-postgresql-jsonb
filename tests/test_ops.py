# -*- coding: utf-8 -*-

from unittest import TestCase, main
from mongo_connector.doc_managers import ops


class TestOps(TestCase):
    def test_update_to_path_and_value(self):
        update_spec = {'$set': {'a.b.c': 1, 'a.c': 2, 'c': 3}}
        actual = ops.update_to_path_and_value(update_spec)
        expected = [('{a,b,c}', 1), ('{a,c}', 2), ('{c}', 3)]
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
