Kafka-Jdbc-Connector Integration test

This integration test uses the Kafka-Jdbc-Connector to insert data from Kafka
to CrateDB with the Postgres-Jdbc-Driver.

The following diagram shows the data-flow:

Kafka -> Kafka-Jdbc-Connector -> Postgres Jdbc Driver -> CrateDB

CrateDB, Kafka, and all related components are provided by a docker image defined
in `dock-compose.yml`

To run the test manually follow the next steps:

```
$ docker-compose up
```

```
gradle test --tests io.crate.qa.kafka.KafkaJdbcConnectorCrateDBIntegrationTest
```