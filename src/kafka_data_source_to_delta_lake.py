# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

import sys

if __name__ == "__main__":
    if len(sys.argv) < 9:
        print("USAGE: %s [input kafka_bootstrap_servers] [subscribe] [startingOffsets] " +
              "[max_offsets_in_batch] [output_path] [checkpoint_path] " +
              "[trigger_interval_secs] [output_partition_num]")
        sys.exit(1)

    input_kafka_bootstrap_servers = sys.argv[1]
    input_kafka_subscribe = sys.argv[2]
    input_kafka_starting_offsets = sys.argv[3]
    max_offsets_in_batch = int(sys.argv[4])

    output_path = sys.argv[5]

    checkpoint_path = sys.argv[6]
    trigger_interval_secs = int(sys.argv[7])
    output_partition_num = int(sys.argv[8])

    print("=" * 40)
    print("Input Kafka bootstrap servers: %s" % input_kafka_bootstrap_servers)
    print("Input kafka topic to subscribe: %s" % input_kafka_subscribe)
    print("Input Kafka starting offsets: %s" % input_kafka_starting_offsets)
    print("Max offsets in batch: %s" % max_offsets_in_batch)
    print("Output path: %s" % output_path)
    print("Checkpoint path: %s" % checkpoint_path)
    print("Trigger interval: %s" % trigger_interval_secs)
    print("Number of output partitions: %s" % output_partition_num)
    print("=" * 40)

    spark = SparkSession \
        .builder \
        .appName("SSKafkaDataSourceToIceberg") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", input_kafka_bootstrap_servers) \
        .option("subscribe", input_kafka_subscribe) \
        .option("startingOffsets", input_kafka_starting_offsets) \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", max_offsets_in_batch) \
        .load()

    if output_partition_num > 0:
        df = df.repartition(output_partition_num)

    query = df \
        .select("value") \
        .writeStream \
        .format("delta") \
        .outputMode("append") \
        .trigger(processingTime="%d seconds" % trigger_interval_secs) \
        .option("checkpointLocation", checkpoint_path) \
        .start(path=output_path, queryName='SS-kafka-datasource-delta-lake-sink-experiment')

    query.awaitTermination()

    print("Query terminated.")
    exc = query.exception()
    if exc:
        print("Exception: %s\n%s" % (exc.desc, exc.stackTrace))

    print("Done...")
