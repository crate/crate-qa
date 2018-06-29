-- dbext:type=CRATE:host=localhost:port=4200:dbname=doc


create table doc.pg_type (
    oid int,
    typdelim string,
    typelem int,
    typname string,
    typtype string,
    typnamespace int,
    typbasetype int
);

insert into doc.pg_type (oid, typdelim, typelem, typname, typtype, typnamespace, typbasetype)
    (select oid, typdelim, typelem, typname, typtype, 11, 0 from pg_catalog.pg_type);
refresh table doc.pg_type;

update doc.pg_type set typname='timestamptz' where typname='timestampz';
update doc.pg_type set typname='_timestamptz' where typname='_timestampz';

create table doc.pg_namespace (
    oid int,
    nspname string
);

insert into doc.pg_namespace (oid, nspname) values (11, 'pg_catalog');
refresh table doc.pg_namespace;

CREATE TABLE doc.users (
  id LONG,
  name STRING,
  value FLOAT
)
