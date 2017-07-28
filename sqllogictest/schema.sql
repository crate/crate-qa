-- drop table if exists qa.logictest;
create table qa.logictest (
 crate_version string not null,
 filename string not null,
 lines integer not null,
 command integer not null,
 success integer not null,
 whitelisted integer not null,
 unsupported integer not null,
 failures integer not null,
 primary key (crate_version, filename)
);
