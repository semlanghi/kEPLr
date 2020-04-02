#!/usr/bin/env bash


export _JAVA_OPTIONS="-Xmx3g"
experiment=$1
broker_count=$2
echo $broker_count
echo $experiment
init_chunk_size=$3
nr_of_chunks=$4
chunk_growth=$5
within=5000
DUMP=$6

if [ -z "$KAFKA_HOME" ]
then
      KAFKA_HOME="/root/confluent-5.4.1"
else
      echo "KAFKA_HOME is $KAFKA_HOME"
fi

if [ -z "$PROJECT_DIR" ]
then
      PROJECT_DIR="/root/kEPLr"
else
      echo "PROJECT_DIR is $PROJECT_DIR"
fi


# stop brokers
echo "Stopping brokers"
for i in $(seq 0 $END)
  do
    $KAFKA_HOME/bin/kafka-server-stop $PROJECT_DIR/configs/server-$i.properties &
  done
$KAFKA_HOME/bin/kafka-server-stop $PROJECT_DIR/configs/server-$((broker_count-1)).properties & sleep 10

# stop zookeeper
echo "Stopping zookeeper"
$KAFKA_HOME/bin/zookeeper-server-stop $KAFKA_HOME/etc/kafka/zookeeper.properties

rm -rf /tmp/