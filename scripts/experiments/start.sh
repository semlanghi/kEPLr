#!/usr/bin/env bash

#export _JAVA_OPTIONS="-Xmx10g"

#MACHINE ID - added to the broker id to differentiate id across machines in the same network
machine_id=0

#Set up variables

# Optional Fields
broker_opt="";
schema_opt=""
name_opt=""
run_opt="--run"
topic_opt=""
output_opt=""
partitions_max_index_opt="--partitions-max-index"
partitions_min_index_opt="--partitions-min-index"
broker_count_opt=""
chunk_growth_opt=""
init_time_opt=""
zookeeper_opt=""

# Mandatory Fields
within_opt=""
chunk_number_opt=""
chunk_size_opt=""

broker=""
schema=""
name=""
run=0
topic=""
output=""
preloaded=false
partitions_max_index=0
partitions_min_index=0
broker_count=0
chunk_size=""
chunk_growth=""
chunk_number=""
within=""
init_time=""
zookeeper=""

# read arguments

if [ -z "$KAFKA_HOME" ]
then
      KAFKA_HOME="/Users/samuelelanghi/Documents/platforms/confluent-5.3.1"
else
      echo "KAFKA_HOME is $KAFKA_HOME"
fi

if [ -z "$PROJECT_DIR" ]
then
      PROJECT_DIR="/Users/samuelelanghi/Documents/projects/kEPLr"
else
      echo "PROJECT_DIR is $PROJECT_DIR"
fi

# read arguments

while [[ $# -gt 0 ]]; do
    case "$1" in
        --broker)
            broker=$2
            broker_opt="--broker"
            shift 2
            ;;
        --machine_id)
            machine_id=$2
            shift 2
            ;;

        --schema)
            schema=$2
            schema_opt="--schema"
            shift 2
            ;;

        --preloaded)
            preloaded=true
            shift 1
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

        --partitions_max_index)
            partitions_max_index=$2
            partitions_max_index_opt="--partitions-max-index"
            shift 2
            ;;

        --partitions_min_index)
            partitions_min_index=$2
            partitions_min_index_opt="--partitions-min-index"
            shift 2
            ;;

        --broker-count)
            broker_count=$2
            broker_count_opt="--broker-count"
            shift 2
            ;;

        --chunk-size)
            chunk_size=$2
            chunk_size_opt="--chunk-size"
            shift 2
            ;;

        --chunk-growth)
            chunk_growth=$2
            chunk_growth_opt="--chunk-growth"
            shift 2
            ;;

        --chunk-number)
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


if [ "${chunk_size}" = "" ]; then
  echo "Mandatory Field chunk-size missing"
  exit
fi

if [ "${chunk_number}" = "" ]; then
  echo "Mandatory Field chunk-number missing"
  exit
fi

if [ "${within}" = "" ]; then
    echo "Mandatory Field within missing"
  exit
fi

if [ "${name}" = "" ]; then
    echo "Mandatory Field name missing"
  exit
fi

if [ ${preloaded} = false ]; then
    #clean logs:
    echo "Cleaning /kafka_log* and zookeeper folders from /tmp"
    rm -rf /tmp/zookeeper;
    rm -rf /tmp/kafka-logs*;

    #start zookeeper if necessary
    if [ "${zookeeper}" == "" ]; then
        echo "Starting zookeeper"
        zookeeper="localhost:2181"
        zookeeper_opt="--zookeeper"
        $KAFKA_HOME/bin/zookeeper-server-start $KAFKA_HOME/etc/kafka/zookeeper.properties &> "./outputs/zoo.out" & sleep 10
    fi



    #start brokers
    echo "Starting brokers"
    if [ ${broker_count} -lt 1 ]; then
        broker_count=1
        broker_count_opt="--broker"
    fi


    for i in $(seq 0 $((broker_count-1)))
      do
        $KAFKA_HOME/bin/kafka-server-start $PROJECT_DIR/configs/server-$((machine_id+i)).properties &> "./outputs/kafka.out" &
      done

    sleep 15

#    $KAFKA_HOME/bin/schema-registry-start $PROJECT_DIR/configs/schema-registry.properties
#    sleep 15
fi


if [ "${topic}" = "" ]; then
    echo "Setting default input topic $name-run-$run."
    topic="$name-run-$run"
    topic_opt="--topic"
fi

if [ "${output}" = "" ]; then
    echo "Setting default output topic $name-run-$run."
    output="output-$name-run-$run"
    output_opt="--output"
fi

if [ ${preloaded} = false ]; then
    # Setup topic
    echo "Setting up producer topic"
    $KAFKA_HOME/bin/kafka-topics ${zookeeper_opt} ${zookeeper} --delete --topic "${topic}" --if-exists
    $KAFKA_HOME/bin/kafka-topics --create ${zookeeper_opt} ${zookeeper} --replication-factor 1 --partitions "$(($partitions_max_index - $partitions_min_index + 1))" --topic "${topic}"
    $KAFKA_HOME/bin/kafka-topics --create ${zookeeper_opt} ${zookeeper} --replication-factor 1 --partitions "$(($partitions_max_index - $partitions_min_index + 1))" --topic "${output}"
    sleep 10
fi

if [ "${broker}" == "" ]; then
    echo "Setting up default bootstrap-server localhost:9092"
    broker="localhost:9092"
    broker_opt="--broker"
fi

echo "Starting application instance for $name"
java -cp $PROJECT_DIR/target/keplr-jar-with-dependencies.jar evaluation.keplr.WorkerMain ${broker_opt} ${broker} ${name_opt} ${name} ${within_opt} ${within} ${run_opt} "${run}" --partitions "$(($partitions_max_index - $partitions_min_index + 1))" ${topic_opt} "${topic}" ${output_opt} "${output}" &> "./outputs/${name}-run-${run}.out" &
sleep 10

if [ ${preloaded} = false ]; then
    # Execute producer
    echo "Starting producer: $name"
    java -cp $PROJECT_DIR/target/keplr-jar-with-dependencies.jar evaluation.producer.ProducerMain ${broker_opt} ${broker} ${schema_opt} ${schema} ${name_opt} ${name} ${run_opt} ${run}  ${topic_opt} ${topic} ${partitions_max_index_opt} ${partitions_max_index}  ${chunk_size_opt} ${chunk_size} ${chunk_growth_opt} ${chunk_growth} ${chunk_number_opt} ${chunk_number} ${within_opt} ${within} ${init_time_opt} ${init_time} ${partitions_min_index_opt} ${partitions_min_index} &> "./outputs/${name}-run-${run}-producer.out" &
    echo "Producer finished"
fi

echo "Starting consumer instance on topic ${output}."
java -cp $PROJECT_DIR/target/keplr-jar-with-dependencies.jar evaluation.consumer.CustomResultDumper ${output} &> "./outputs/result-${name}-run-${run}".out &



