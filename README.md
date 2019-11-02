Huskar API
==========

[![CircleCI](https://circleci.com/gh/huskar-org/huskar/tree/master.svg?style=svg)](https://circleci.com/gh/huskar-org/huskar/tree/master)
[![codecov](https://codecov.io/gh/huskar-org/huskar/branch/master/graph/badge.svg)](https://codecov.io/gh/huskar-org/huskar)

HTTP API of Huskar.

How to start
------------

    $ cp .env.example .env && vim .env

Starting the API server in local environment:

    $ . path/to/venv/activate
    $ make install-deps
    $ honcho start

Starting the API server in [Docker](https://www.docker.com/products/docker):

    $ docker-compose run --rm wsgi initdb      # initialize database
    $ docker-compose run --rm wsgi initadmin   # initialize administrator
    $ docker-compose up wsgi                   # start web server


Development FAQ
---------------

Using the ZooKeeper CLI:

    $ zkCli -server $(docker-compose port zookeeper 2181)

Using the MySQL CLI:

    $ mycli mysql://root@$(docker-compose port mysql 3306)/huskar_api

Updating dependencies:

    $ docker-compose run --rm wsgi make compile-deps
    $ git add -p requirements*

Running tests:

    $ docker-compose run --rm wsgi testall -xv
    $ docker-compose run --rm wsgi test test_foo.py -xv

Maintaining database schema:

    $ docker-compose run --rm wsgi alembic upgrade head
    $ vim huskar_api/models/foobar.py
    $ docker-compose run --rm wsgi alembic revision --autogenerate -m 'add an index of foo'
    $ vim database/migration/versions/xxxxxxx.py
    $ docker-compose run --rm wsgi alembic upgrade head
    $ docker-compose run --rm wsgi dumpdb
    $ git add database

Updating snapshot of email template in tests:

    $ docker-compose run --rm wsgi python run.py tests.test_extras.test_email:gen
