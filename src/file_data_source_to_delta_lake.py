# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

import sys

if __name__ == "__main__":
    if len(sys.argv) < 7:
        print("USAGE: %s [input_path] [max_files_in_batch] [output_path] [checkpoint_path] [trigger_interval_secs] [output_partition_num]" % sys.argv[0])
        sys.exit(1)

    input_path = sys.argv[1]
    max_files_in_batch = int(sys.argv[2])
    output_path = sys.argv[3]
    checkpoint_path = sys.argv[4]
    trigger_interval_secs = int(sys.argv[5])
    output_partition_num = int(sys.argv[6])

    print("=" * 40)
    print("Input path: %s" % input_path)
    print("Max files in batch: %s" % max_files_in_batch)
    print("output path: %s" % output_path)
    print("Checkpoint path: %s" % checkpoint_path)
    print("Trigger interval: %s" % trigger_interval_secs)
    print("Number of output partitions: %s" % output_partition_num)
    print("=" * 40)

    spark = SparkSession \
        .builder \
        .appName("SSFileDataSourceWithDeltaLakeSink") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    df = spark \
        .readStream \
        .format("text") \
        .option("path", input_path) \
        .option("maxFilesPerTrigger", max_files_in_batch) \
        .load()

    if output_partition_num > 0:
        df = df.repartition(output_partition_num)

    query = df \
        .writeStream \
        .format("delta") \
        .outputMode("append") \
        .trigger(processingTime="%d seconds" % trigger_interval_secs) \
        .option("checkpointLocation", checkpoint_path) \
        .start(path=output_path, queryName='SS-file-datasource-delta-lake-sink-experiment')

    query.awaitTermination()

    print("Query terminated.")
    exc = query.exception()
    if exc:
        print("Exception: %s\n%s" % (exc.desc, exc.stackTrace))

    print("Done...")
