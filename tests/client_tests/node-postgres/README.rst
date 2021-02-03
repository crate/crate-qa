####################################
CrateDB client tests (node-postgres)
####################################


*****
About
*****
A nodejs_ application showcasing a test interaction with **CrateDB**.


********
Synopsis
********
Start the node-postgres_ module as middleware.

Run it like:

- `npm install` (required once, to fetch the dependencies)
- `node main.js <host> <port>` (requires **CrateDB** to be running on <host>:<port>).
- `./run.sh` (takes care of running **CrateDB** by means of cr8_).


************
Using Docker
************
In order to run CrateDB on Docker, try::

    # Run the most recent "nightly" build of CrateDB.
    docker run -it --rm --publish 5432:5432 crate/crate:nightly

    # Run a specific version of an official release of CrateDB.
    docker run -it --rm --publish 5432:5432 crate:4.3.4

    # Invoke test suite.
    node --trace-warnings main.js localhost 5432


*****************
Switching Node.js
*****************
When aiming at different versions of Node.js, you might want to use nodeenv_ like::

    # Do everything within a dedicated virtualenv.
    python3 -m venv .venv
    source .venv/bin/activate

    # Setup Node.js 15.6.0 within an isolated Node.js environment.
    pip install nodeenv
    nodeenv --node=15.6.0 .nenv
    source .nenv/bin/activate

    # Verify you are running the expected version of Node.js.
    node --version
    v15.6.0

    # Invoke test suite.
    node --trace-warnings main.js localhost 5432


.. _nodejs: https://nodejs.org/en/
.. _node-postgres: https://node-postgres.com/
.. _cr8: https://github.com/mfussenegger/cr8
.. _nodeenv: https://pypi.org/project/nodeenv/
