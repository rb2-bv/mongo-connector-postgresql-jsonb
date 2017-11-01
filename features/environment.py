import os
import psutil
import subprocess
from pymongo import MongoClient


MONGO_URL = 'mongodb://localhost:27018'
POSTGRES_URL = 'postgresql://username:password@localhost:5442/target'


def before_all(context):
    print('Loading config in before_all')
    context.mongo_url = os.getenv('MONGO_URL', MONGO_URL)
    context.postgres_url = os.getenv('POSTGRES_URL', POSTGRES_URL)
    context.project_root = os.path.abspath(os.path.join(os.path.realpath(__file__), os.pardir, os.pardir))
    cmd = 'PYTHONPATH={} mongo-connector -m {} -t {} -n \'database.collection1\' -d postgresql_jsonb_manager -v'.format(
        context.project_root, context.mongo_url, context.postgres_url
    )
    print('Starting connector with command {}'.format(cmd))
    context.mongo_connector = subprocess.Popen(
        cmd,
        shell=True,
        cwd=context.project_root
    )


def before_scenario(context, scenario):
    client = MongoClient(context.mongo_url)
    mongo_db = client.get_database('database')
    mongo_db['collection1'].delete_many({})
    client.close()


def after_all(context):
    print('Killing child processes')
    current_process = psutil.Process()
    for child in current_process.children(recursive=True):
        print('Killing child process {}'.format(child.pid))
        child.kill()
