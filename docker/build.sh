#!/bin/bash
# Run Unit & Integration Tests (inside a docker container)
set -e
cd /src
rm oplog.timestamp || true # in case of running locally
pip3 install --no-cache-dir -r ./requirements.txt
python3 setup.py test
python3 setup.py install
behave