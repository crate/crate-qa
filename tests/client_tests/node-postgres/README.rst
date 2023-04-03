####################################
CrateDB client tests (node-postgres)
####################################

Tests ensuring node-postgres works with CrateDB.


Setup
=====

- ``npm install`` (required once, to fetch the dependencies)

Usage
=====

The tests expect a CrateDB instance to be running on localhost:5432.

Start CrateDB locally. For example using Docker::

    # Run the recent "nightly" build of CrateDB.
    docker run -it --rm --publish 5432:5432 crate/crate:nightly

    # Run a specific version of an official release of CrateDB.
    docker run -it --rm --publish 5432:5432 crate:4.3.4

Run tests::

    npm test


Or use ``run.sh`` which starts CrateDB using `cr8`_ and runs the tests.


.. _nodejs: https://nodejs.org/en/
.. _node-postgres: https://node-postgres.com/
.. _cr8: https://github.com/mfussenegger/cr8
