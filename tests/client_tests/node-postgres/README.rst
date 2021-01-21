====================================
CrateDB client tests (node-postgres)
====================================

A nodejs_ application showcasing a test interaction with **CrateDB**.

Starting the node-postgres_ module as middleware.

Run it like:

- `npm install` (required once, to fetch the dependencies)
- `node main.js <host> <port>` (requires **CrateDB** to be running on <host>:<port>).
- `./run.sh` (takes care of running **CrateDB** by means of cr8_).

When running CrateDB through Docker, try::

	docker run -it --rm --publish 5432:5432 crate:4.3.3
	node --trace-warnings main.js localhost 5432


.. _nodejs: https://nodejs.org/en/
.. _node-postgres: https://node-postgres.com/
.. _cr8: https://github.com/mfussenegger/cr8
