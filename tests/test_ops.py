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
        expected = {'foo': {'bar':'baz','box':'baz'}}
        self.assertEqual(actual, expected)


if __name__ == '__main__':
    main()
