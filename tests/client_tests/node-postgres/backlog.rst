################################################
Backlog for CrateDB client tests (node-postgres)
################################################


Use a real testing framework
============================

We should use a real testing framework for improved structure, test case
discovery and better reporting. It will probably also deliver a mechanism
for measuring code coverage and other features.

Currently, the tests are based on Chai_, which is essentially just an
assertion library. Chai_ can be accompanied by Mocha_ to build a whole
test suite framework, see `How to make tests using chai and mocha?`_.

On the other hand, the `tests for node-postgres`_ are based on Lerna_.


.. _Chai: https://www.chaijs.com/
.. _Mocha: https://mochajs.org/
.. _How to make tests using chai and mocha?: https://itnext.io/how-to-make-tests-using-chai-and-mocha-e9db7d8d48bc

.. _tests for node-postgres: https://github.com/brianc/node-postgres/tree/master/packages/pg/test
.. _Lerna: https://github.com/lerna/lerna
