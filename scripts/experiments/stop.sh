#!/usr/bin/env bash

zookeeper_also=true
broker_count=1

if [ -z "$KAFKA_HOME" ]
then
      KAFKA_HOME="/Users/samuelelanghi/Documents/platforms/kafka_2.13-3.1.0"
else
      echo "KAFKA_HOME is $KAFKA_HOME"
fi


while [[ $# -gt 0 ]]; do
    case "$1" in
        --broker-count)
            broker_count=$2
            shift 2
            ;;
        --zookeeper-also)
            zookeeper_also=$2
            shift 2
            ;;
        *)
            echo "There are some wrong Options."
            exit 1
            break
            ;;
    esac
done


# stop brokers
echo "Stopping brokers"
for i in $(seq 1 $((broker_count)))
  do
    echo "Stopping Broker number $i."
    $KAFKA_HOME/bin/kafka-server-stop.sh &
  done
sleep 10

# stop zookeeper
if [ $zookeeper_also = true ]; then
    echo "Stopping zookeeper"
    $KAFKA_HOME/bin/zookeeper-server-stop.sh &
fi
sleep 10

#clean logs:
echo "Cleaning /kafka_log* and zookeeper folders from /tmp"
rm -rf /tmp/zookeeper;
rm -rf /tmp/kafka-logs*;
