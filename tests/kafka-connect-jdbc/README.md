Launch Zookeeper, Kafka, schema-registry, kafka-connect and CrateDB:


```
$ docker-compose up
```


Create a table:


```
crash --hosts localhost:4200 <<EOF
  create table metrics (id int primary key, x int);
EOF

```


Configure a JDBC sink.

Inserts only:

```bash
http PUT localhost:8083/connectors/jdbc-sink-connector/config \
  connector.class="io.confluent.connect.jdbc.JdbcSinkConnector" \
  topics='metrics' \
  connection.url='jdbc:postgresql://localhost:5432/doc?user=crate' \
  tasks.max=1
```


With Upsert:

```bash
http PUT localhost:8083/connectors/jdbc-sink-connector/config \
  connector.class="io.confluent.connect.jdbc.JdbcSinkConnector" \
  topics='metrics' \
  connection.url='jdbc:postgresql://cratedb:5432/doc?user=crate' \
  tasks.max=1 \
  pk.mode=record_key \
  pk.fields=id \
  insert.mode=upsert
```


See [Sink connector options](https://docs.confluent.io/current/connect/kafka-connect-jdbc/sink-connector/sink_config_options.html#sink-pk-config-options for further options)


Insert some data into Kafka:

[JSON with schema](https://rmoff.net/2017/09/06/kafka-connect-jsondeserializer-with-schemas.enable-requires-schema-and-payload-fields/):


```bash
kafkacat -b localhost:9092 -t metrics -K "|" -P <<EOF
1|{"schema": {"type": "struct", "fields": [{"type": "int32", "field": "id"}, {"type": "int32", "field": "x"}]}, "payload": {"id": 1, "x": 45}}
2|{"schema": {"type": "struct", "fields": [{"type": "int32", "field": "id"}, {"type": "int32", "field": "x"}]}, "payload": {"id": 2, "x": 20}}
EOF
```
