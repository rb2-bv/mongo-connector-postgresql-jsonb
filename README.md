# Mongo to Postgres Connector

**NOTE: this connector is incomplete and requires more work.**

## Description

A connector, built on
[Mongo Connector](https://github.com/mongodb-labs/mongo-connector),
to migrate data from Mongo to Postgres.

For each specified collection, you must create the following schema in Postgres:

    create table [TABLE] (id string PRIMARY KEY, jdoc jsonb)
    create index [TABLE]_jdoc_gin on [TABLE] using GIN (jdoc)

Where `[TABLE]` is the Mongo collection name. 

E.g. for the collection `audit.accountHistory` you need to create a table called `accountHistory` and an index `accountHistory_jdoc_gin`.

## Running

Create a
[config.json](https://github.com/mongodb-labs/mongo-connector/wiki/Configuration-Options)
file like:

    {
        "mainAddress": "localhost:27017",
        "docManagers": [
            {
                "docManager": "postgresql_jsonb_manager",
                "targetURL": "postgresql://localhost:5432/dms",
                "args": { "mongoUrl": "localhost:27017" }
            }
        ]
    }

There are lots of additional options but the ones here are the bare
minimum.

Then run:

     mongo-connector -c config.json

## Testing and local development

### Unit Tests

- Run `python3 setup.py test`

### Integration Tests

You'll need:

* a running mongo instance as source
* a running Postgres as target
* `[behave](http://pythonhosted.org/behave/)` (`pip install behave`)
* `docker`, `docker-compose`

Optionally, but recommended:

* [pipenv](https://docs.pipenv.org/en/latest/) to manage Python
dependencies (a good guide also
[here](http://docs.python-guide.org/en/latest/dev/virtualenvs/)

### Start dependencies with docker

* from the project root run `docker-compose up -d`.
* postgres will bind to port `5442` and mongodb to `27017`.
* a postgres database `target` will be created, and a user `username` with password `password`

### Run the tests

Run the integration tests: 

    behave
    
By default the integration tests will target `mongodb://localhost:27017` and `postgresql://username:password@localhost:5442/target`.
    
If not using docker to run the dependencies as described above you may pass custom mongo and postgres connection strings using the environment variables `MONGO_URL` and `POSTGRES_URL`: 
    
    MONGO_URL=mongodb://rahil:qwerty123@somehost.com:27773 POSTGRES_URL=postgresql://rahil:qwerty123@someotherhost.com:2143/customtargetdb behave
    
Finally clean up the docker containers we ran: 

    docker-compose down
    
    
## What is a 'DocManager'

Mongo Connector uses a doc manager to manage replication. Our one
writes Mongo records to Postgres in a jsonb format.

You can read more about what a doc manager is here:

https://github.com/mongodb-labs/mongo-connector/wiki/Writing-Your-Own-DocManager

But essentially, the steps are, create these files:

    docman_project/mongo_connector/__init__.py
    docman_project/mongo_connector/doc_managers/__init__.py
    docman_project/mongo_connector/doc_managers/your_custom_doc_manager.py

The __init__ files tells Python the directory contains a package so it
looks for files in the directory.

The `your_custom_doc_manager.py` file is where your plugin will live.

Then define a class in `your_custom_doc_manager.py` that extends
DocManagerBase and implements a variety of methods around replication
events (the docs describe these in more detail).

Once done, you can test your doc manager. To do this, you'll need to
add you local files to your Python path. Pipenv can help here:

    pipenv shell

From here, assuming you've already pipenv installed things, you can
run mongo-connector as normal:

    mongo-connector -c config.json

Only now, it will use your local files!file for the source etc.

## Thanks

Originally forked from:
https://github.com/Hopwork/mongo-connector-postgresql so thanks to
Hopwork and the original authors. :)
