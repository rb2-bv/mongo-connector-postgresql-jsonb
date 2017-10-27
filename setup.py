#!/usr/bin/env python
# coding: utf8

from setuptools import setup

setup(
        name='mongo-connector-postgresql-jsonb',
        version='0.0.1',
        description='Doc Manager Postgresl for Mongo connector Distribution Utilities',
        keywords=['mongo-connector', 'mongo', 'mongodb', 'postgresql'],
        platforms=["any"],
        author='The Guardian',
        author_email='identitydev@guardian.co.uk',
        install_requires=[
            'mongo_connector >= 2.5.1',
            'psycopg2 >= 2.7.3.2'
        ],
        tests_require=['mock>=2.0.0'],
        license="http://www.apache.org/licenses/LICENSE-2.0.html",
        url='https://github.com/guardian/mongo-connector-postgresql-jsonb',
        packages=["mongo_connector", "mongo_connector.doc_managers"],
        test_suite='tests'
)
