-- dbext:type=CRATE:host=localhost:port=4200:dbname=doc

-- Provision a database fixture for the test case scenario defined in this folder.

DROP TABLE IF EXISTS doc.testdrive;

CREATE TABLE doc.testdrive (
  id LONG,
  name STRING,
  value FLOAT
);

INSERT INTO doc.testdrive
 (
  id,
  name,
  value
) VALUES (
  1,
  'foo',
  42.42
);

REFRESH TABLE doc.testdrive;
