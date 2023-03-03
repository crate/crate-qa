############################
Evaluate CrateDB with pandas
############################


*****
About
*****

A little setup to evaluate CrateDB with pandas. Specifically, we are looking at
the `read_sql`_ and `to_sql`_ features of `pandas`_. The setup has two
operational modes:

- Ad hoc: Tests will run directly within a development sandbox on your machine.
  Needs an external CrateDB service, for example provided through Docker.
  Suitable for development.

- Dockerized: Run everything in a box. CrateDB will be started using `cr8`_.
  Suitable for running on Jenkins, like other QA test scenarios.


******
Ad hoc
******

Setup environment::

    docker run --rm -it --publish=4200:4200 --publish=5432:5432 crate/crate:nightly
    python3 -m venv .venv
    source .venv/bin/activate
    pip install --requirement=requirements.txt --requirement=requirements-dev.txt

Run tests::

    poe test

If you need more control to adjust the location of CrateDB, use those options::

    pytest -vvv --http-host 127.0.0.1 --http-port 4200 --psql-host 127.0.0.1 --psql-port 5432


**********
Dockerized
**********

In order to test the Dockerized setup, just invoke::

    ./test.sh

On Jenkins, where the Docker container will get spawned by the agent plugin
already, the corresponding entrypoint is::

    ./run.sh


.. _cr8: https://github.com/mfussenegger/cr8
.. _pandas: https://pandas.pydata.org/
.. _read_sql: https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html
.. _to_sql: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html
