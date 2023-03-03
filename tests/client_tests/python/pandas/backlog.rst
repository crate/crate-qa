##########################
CrateDB <-> pandas backlog
##########################


About
=====

A few things have been unlocked, some others are still blocked. Sort this out,
and create corresponding issues on the `CrateDB issue tracker`_.

General
=======

- Use user-cache path ``/Users/amo/.cache/cr8/crates`` instead of project-local ``.cache``?
  => cr8 needs a little adjustment.


``read_sql``
============

- https://pypi.org/project/sqlalchemy-postgresql-relaxed/


``to_sql``
==========

- ``COLLATE`` in ``sqlalchemy.dialects.postgresql.base`` and ``sqlalchemy.sql.compiler``
- ``pg_catalog.pg_table_is_visible``:
  ``sqlalchemy.exc.InternalError: (psycopg.errors.InternalError_) Unknown function:
  pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid)``


.. _CrateDB issue tracker: https://github.com/crate/crate/issues
