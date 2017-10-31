# Mongo to Postgres Connector

**NOTE: this connector is incomplete and requires more work.**

## Description

A connector, built on
[Mongo Connector](https://github.com/mongodb-labs/mongo-connector),
to migrate data from Mongo to Postgres.

For each specified collection, the connector creates the following
schema in Postgres:

    create database [DATABASE]
    create table [TABLE] (id string PRIMARY KEY, jdoc jsonb)
    create index [TABLE]_jdoc_gin on [TABLE] using GIN (jdoc)

where [TABLE] is the Mongo collection name.

It is recommended to create a specific user for the connector to use. The
simplest way to do this is:

    $ create user [USER_NAME] CREATEDB LOGIN;

As this user will be used to create the target database it will own
all of the contained resources. Once done, you may want to remove the
CREATEDB permission:

    $ alter user [USER_NAME] NOCREATEDB

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

Optionally, but recommended:

* [pipenv](https://docs.pipenv.org/en/latest/) to manage Python
dependencies (a good guide also
[here](http://docs.python-guide.org/en/latest/dev/virtualenvs/)

#### Mongo

To setup a replica set locally for testing, install Mongo locally and start as a replica set:

    $ mongod --replSet singleNodeRepl

Then, in the mongo shell:

    rsconf = {
        _id: "singleNodeRepl",
        members: [
            {
                _id: 0,
                host: "localhost:27017"
            }
        ]
    }

    rs.initiate(rsconf)

Create a mongo user for user by the integration tests:
    
    db.createUser({user: "connector", pwd: "password", roles: [{role: "read", db: "local"}, {role: "dbAdmin", db: "database"}]})
    
#### Postgres

* Run postgresql on port 5432 and create an admin user with the username `username` and password `password`. 
* Create the target database `target`: `create database target;`

### Run the tests

Run the integration tests: 

    behave
    
You may pass non standard mongo and postgres connection strings using the enviroment variables `MONGO_URL` and `POSTGRES_URL`: 
    
    MONGO_URL=mongodb://rahil:qwerty123@somehost.com:27773 POSTGRES_URL=postgresql://rahil:qwerty123@someotherhost.com:2143/customtargetdb behave


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
