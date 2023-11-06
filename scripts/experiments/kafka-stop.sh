#!/usr/bin/env bash

zookeeper_also=true
broker_count=1

# Environment Variable Check
if [ -z "$KAFKA_HOME" ]
then
      echo "You need to set environment variable KAFKA_HOME with the kafka folder (confluent folder is NOT supported)."
      exit
fi


while [[ $# -gt 0 ]]; do
    case "$1" in
        --help)
            echo "
            --broker-count: how many brokers to stop, max is 3
            --zookeeper-also: to stop also zookeeper, we assume only one instance of zookeeper"
            exit
            ;;
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
    wait
  done

# stop zookeeper
if [ $zookeeper_also = true ]; then
    echo "Stopping zookeeper"
    $KAFKA_HOME/bin/zookeeper-server-stop.sh &
    wait
fi

#clean logs:
echo "Cleaning /kafka_log* and zookeeper folders from /tmp"
rm -rf /tmp/zookeeper;
rm -rf /tmp/kafka-logs*;
