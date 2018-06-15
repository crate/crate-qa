-- dbext:type=CRATE:host=localhost:port=4200:dbname=doc


create table doc.pg_type (
    oid int,
    typdelim string,
    typelem int,
    typname string,
    typtype string
);

insert into doc.pg_type (oid, typdelim, typelem, typname, typtype)
    (select oid, typdelim, typelem, typname, typtype from pg_catalog.pg_type);

CREATE TABLE doc.users (
  id LONG,
  name STRING,
  value FLOAT
)
