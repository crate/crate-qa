#!/usr/bin/env bash

cd "$(dirname "$0")"

cratedb_image_id=$(docker images | grep crate/crate | awk '{ print $3 }')

if [[ ${cratedb_image_id} ]] ; then
	echo "Remove cached crate docker image"
	docker rmi ${cratedb_image_id}
fi

docker-compose up -d

echo "Wait until kafka-jdbc-connector is started up!"

KAFKA_CONNECTOR_URL="localhost:8083/"
MAX_ATTEMPTS=10
attempt=0
timeout=1
status_code=0
exit_code=0

while (( $attempt < $MAX_ATTEMPTS )) ; do
  status_code=$(curl --write-out %{http_code} --silent --output /dev/null ${KAFKA_CONNECTOR_URL})

  if [[ ${status_code} -ne 200 ]] ; then
    echo "Try to connect to kafka-jdbc-connector ${KAFKA_CONNECTOR_URL}"
    sleep ${timeout}
    attempt=$(( attempt + 1 ))
    timeout=$(( timeout * 2 ))
  else
    curl -I ${KAFKA_CONNECTOR_URL}
    echo "Successfully connected to kafka-jdbc-connector ${KAFKA_CONNECTOR_URL}"
    break
  fi
done

if [[ ${status_code} != 200 ]] ; then
  echo "Failed to connect to kafka-jdbc-connector ${KAFKA_CONNECTOR_URL}"
  exit_code=1
fi

if ! ./gradlew clean test --tests io.crate.qa.kafka.KafkaJdbcConnectorCrateDBIntegrationTest ; then
  echo "Failed executing kafka-jdbc-connector test"
  docker-compose logs kafka-connect
  exit_code=1
fi

docker-compose down
exit ${exit_code}