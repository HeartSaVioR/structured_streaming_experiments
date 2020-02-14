# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

import sys

if __name__ == "__main__":
    if len(sys.argv) < 9:
        print("USAGE: %s [input kafka_bootstrap_servers] [subscribe] [startingOffsets] " +
              "[output kafka_bootstrap_servers] [output kafka topic] " +
              "[checkpoint_path] [trigger_interval_secs] [output_partition_num]")
        sys.exit(1)

    input_kafka_bootstrap_servers = sys.argv[1]
    input_kafka_subscribe = sys.argv[2]
    input_kafka_startingOffsets = sys.argv[3]

    output_kafka_bootstrap_servers = sys.argv[4]
    output_kafka_topic = sys.argv[5]

    checkpoint_path = sys.argv[6]
    trigger_interval_secs = int(sys.argv[7])
    output_partition_num = int(sys.argv[8])

    spark = SparkSession \
        .builder \
        .appName("SSKafkaDataSource") \
        .getOrCreate()

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", input_kafka_bootstrap_servers) \
        .option("subscribe", input_kafka_subscribe) \
        .option("startingOffsets", input_kafka_startingOffsets) \
        .option("failOnDataLoss", "false") \
        .load()

    if output_partition_num > 0:
        df = df.repartition(output_partition_num)

    query = df \
        .writeStream \
        .format("kafka") \
        .outputMode("append") \
        .trigger(processingTime="%d seconds" % trigger_interval_secs) \
        .option("kafka.bootstrap.servers", output_kafka_bootstrap_servers) \
        .option("topic", output_kafka_topic) \
        .option("checkpointLocation", checkpoint_path) \
        .start(queryName='SS-kafka-datasource-experiment')

    query.awaitTermination()

    print("Query terminated.")
    exc = query.exception()
    if exc:
        print("Exception: %s\n%s" % (exc.desc, exc.stackTrace))

    print("Done...")