# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

import sys

if __name__ == "__main__":
    if len(sys.argv) < 6:
        print("USAGE: %s [input_path] [output_path] [checkpoint_path] [trigger_interval_secs] [output_partition_num]" % sys.argv[0])
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    checkpoint_path = sys.argv[3]
    trigger_interval_secs = int(sys.argv[4])
    output_partition_num = int(sys.argv[5])

    print("=" * 40)
    print("Input path: %s" % input_path)
    print("Output path: %s" % output_path)
    print("Checkpoint path: %s" % checkpoint_path)
    print("Trigger interval: %s" % trigger_interval_secs)
    print("Number of output partitions: %s" % output_partition_num)
    print("=" * 40)

    spark = SparkSession \
        .builder \
        .appName("SSFileDataSource") \
        .getOrCreate()

    df = spark \
        .readStream \
        .format("text") \
        .option("path", input_path) \
        .load()

    if output_partition_num > 0:
        df = df.repartition(output_partition_num)

    query = df \
        .writeStream \
        .format("text") \
        .outputMode("append") \
        .trigger(processingTime="%d seconds" % trigger_interval_secs) \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .start(queryName='SS-file-datasource-experiment')

    query.awaitTermination()

    print("Query terminated.")
    exc = query.exception()
    if exc:
        print("Exception: %s\n%s" % (exc.desc, exc.stackTrace))

    print("Done...")
