#!/usr/bin/env bash

export _JAVA_OPTIONS="-Xmx1g"

# Experiment Configuration
# chunk-size for W1, for W2, etc... the quantity is put to the power of two and divided by 4, since W1 results are many more
# total events = (chunk-size + chunk_growth*(1+2+...+chunk_number))*chunk_number
name="all"
chunk_sizes=(100 500)
chunk_numbers=(10 20)
partitions_counts=(1)

# Optional Fields with Default values
broker="localhost:9092"
chunk_growth=0


if [ -z "$KAFKA_HOME" ]
then
      echo "You need to set environment variable KAFKA_HOME with the kafka folder (confluent folder is NOT supported)."
      exit
fi

# read arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --help)
            echo "
            --broker: broker address, default 'localhost:9092'
            --name: experiment name, 'W1', 'W2', 'W3', 'W4', or 'all' for running all of them, default 'all'
            --chunk-growth: growth factor for each new chunk, default 0, i.e., all chunks of the same size"
            exit
            ;;
        --broker)
            broker=$2
            shift 2
            ;;
        --name)
            name=$2
            shift 2
            ;;

        --chunk-growth)
            chunk_growth=$2
            shift 2
            ;;
        *)
            echo "There are some wrong Options."
            exit 1
            break
            ;;
    esac
done

if [ "${name}" = "all" ]; then
  experiments=("W1" "W2" "W3" "W4")
else
  experiments=("${name}")
fi

for experiment in "${experiments[@]}"; do
  for chunk_number in "${chunk_numbers[@]}"; do
    for chunk_size in "${chunk_sizes[@]}"; do
      for partitions_count in "${partitions_counts[@]}"; do
    #    if [ "${experiment}" != "W1" ]; then
    #        chunk_size=$((chunk_size * chunk_size / 4))
    #    fi
        topic="topic-${experiment}-chunkn-$chunk_number-chunks-$chunk_size-chunkg-$chunk_growth-pcount-$partitions_count"
        # Setup topic
        echo "Setting up producer 'topic-${experiment}-chunkn-$chunk_number-chunks-$chunk_size-chunkg-$chunk_growth-pcount-$partitions_count'"
        $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server ${broker} --delete --topic "${topic}" --if-exists
        wait
        $KAFKA_HOME/bin/kafka-topics.sh --create --bootstrap-server ${broker} --replication-factor 1 --partitions "${partitions_count}" --topic "${topic}"
        wait
        echo "Starting producing to created topic"
        echo java -cp ../../target/keplr-jar-with-dependencies.jar evaluation.producer.ProducerMain --broker ${broker} --name ${experiment} --topic ${topic} --partitions-count ${partitions_count}  --chunk-size ${chunk_size} --chunk-growth ${chunk_growth} --chunk-number ${chunk_number}
        java -cp ../../target/keplr-jar-with-dependencies.jar evaluation.producer.ProducerMain --broker ${broker} --name ${experiment} --topic ${topic} --partitions-count ${partitions_count}  --chunk-size ${chunk_size} --chunk-growth ${chunk_growth} --chunk-number ${chunk_number} &> "./outputs/${experiment}--producer.out" &
        echo "Producer finished"
        wait
      done
    done
  done
done


