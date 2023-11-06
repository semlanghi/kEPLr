#!/bin/bash

export _JAVA_OPTIONS="-Xmx5g"

# Environment Variable Check
if [ -z "$KAFKA_HOME" ]
then
      echo "You need to set environment variable KAFKA_HOME with the kafka folder (confluent folder is NOT supported)."
      exit
fi

# List all topics and delete them, except "consumer_offset"
$KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server localhost:9092 | while read topic; do
  if [ "$topic" != "__consumer_offsets" ]; then
    echo "Deleting topic: $topic"
    $KAFKA_HOME/bin/kafka-topics.sh --delete --topic $topic --bootstrap-server localhost:9092
  else
    echo "Skipping topic: $topic"
  fi
done
