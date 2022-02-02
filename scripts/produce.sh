#!/bin/bash

#Set up variables

broker_opt="";
schema_opt=""
name_opt=""
run_opt=""
topic_opt=""
partitions_opt=""
broker_count_opt=""
#Mandatory Field
chunk_size_opt=""
chunk_growth_opt=""
#Mandatory Field
chunk_number_opt=""
#Mandatory Field
within_opt=""
init_time_opt=""

broker=""
schema=""
name=""
run=""
topic=""
partitions=""
broker_count=""
chunk_size=""
chunk_growth=""
chunk_number=""
within=""
init_time=""
zookeeper=""

# read arguments

while [[ $# -gt 0 ]]; do
    case "$1" in
        --broker)
            broker=$2
            broker_opt="--broker"
            shift 2
            ;;

        --schema)
            schema=$2
            schema_opt="--schema"
            shift 2
            ;;

        --name)
            name=$2
            name_opt="--name"
            shift 2
            ;;

        --run)
            run=$2
            run_opt="--run"
            shift 2
            ;;

        --topic)
            topic=$2
            topic_opt="--topic"
            shift 2
            ;;

        --partitions)
            partitions=$2
            partitions_opt="--partitions"
            shift 2
            ;;

        --broker-count)
            broker_count=$2
            broker_count_opt="--broker-count"
            shift 2
            ;;

        --chunksize)
            chunk_size=$2
            chunk_size_opt="--chunk-size"
            shift 2
            ;;

        --chunk-growth)
            chunk_growth=$2
            chunk_growth_opt="--chunk-growth"
            shift 2
            ;;

        --chunknumber)
            chunk_number=$2
            chunk_number_opt="--chunk-number"
            shift 2
            ;;

        --within)
            within=$2
            within_opt="--within"
            shift 2
            ;;

        --init-time)
            init_time=$2
            init_time_opt="--init-time"
            shift 2
            ;;

        --zookeeper)
            zookeeper=$2
            shift 2
            ;;
        *)
            echo "There are some wrong Options."
            exit 1
            break
            ;;
    esac
done

if [ -z "$KAFKA_HOME" ]
then
      KAFKA_HOME="/Users/samuelelanghi/Documents/platforms/confluent-5.3.1"
else
      echo "KAFKA_HOME is set to $KAFKA_HOME"
fi

if [ "${topic}" = "" ]; then
  topic_temp="${name}-${partitions}"
else
  topic_temp=topic
fi

if [ "${chunk_size}" = "" ]; then
  echo "Mandatory Field chunk_size missing"
  exit
fi

if [ "${chunk_number}" = "" ]; then
  echo "Mandatory Field chunk_number missing"
  exit
fi

if [ "${within}" = "" ]; then
    echo "Mandatory Field window missing"
  exit
fi

# Setup topic
$KAFKA_HOME/bin/kafka-topics --zookeeper ${zookeeper} --delete --topic ${topic_temp} --if-exists
$KAFKA_HOME/bin/kafka-topics --zookeeper ${zookeeper} --create --replication-factor 1 --partitions ${partitions} --topic ${topic_temp}

# Execute producer
java -cp ../target/keplr-jar-with-dependencies.jar evaluation.producer.ProducerMain ${broker_opt} ${broker} ${schema_opt} ${schema} ${name_opt} ${name} ${run_opt} ${run}  ${topic_opt} ${topic} ${partitions_opt} ${partitions}  ${broker_count_opt} ${broker_count} ${chunk_size_opt} ${chunk_size} ${chunk_growth_opt} ${chunk_growth} ${chunk_number_opt} ${chunk_number} ${within_opt} ${within} ${init_time_opt} ${init_time}