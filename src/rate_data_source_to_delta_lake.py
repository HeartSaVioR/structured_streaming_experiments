# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

import sys

if __name__ == "__main__":
    if len(sys.argv) < 6:
        print("USAGE: %s [input_records_per_second] [output_path] [table_name] "
              "[checkpoint_path] [trigger_interval_secs] [output_partition_num]")
        sys.exit(1)

    input_records_per_second = sys.argv[1]

    output_path = sys.argv[2]
    table_name = sys.argv[3]

    checkpoint_path = sys.argv[4]
    trigger_interval_secs = int(sys.argv[5])
    output_partition_num = int(sys.argv[6])

    print("=" * 40)
    print("Input records per second: %s" % input_records_per_second)
    print("Output path: %s" % output_path)
    print("Table name: %s" % table_name)
    print("Checkpoint path: %s" % checkpoint_path)
    print("Trigger interval: %s" % trigger_interval_secs)
    print("Number of output partitions: %s" % output_partition_num)
    print("=" * 40)

    spark = SparkSession \
        .builder \
        .appName("SSRateDataSourceToDeltaLake") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    spark.sql("""
        CREATE TABLE IF NOT EXISTS {} (
            `timestamp` timestamp COMMENT 'timestamp',
            `value` string COMMENT 'content',
        )
        USING delta
        LOCATION '{}'
        """.format(table_name, output_path))

    df = spark \
        .readStream \
        .format("rate") \
        .option("rowsPerSecond") \
        .load()

    if output_partition_num > 0:
        df = df.repartition(output_partition_num)

    query = df \
        .selectExpr("value", "timestamp") \
        .writeStream \
        .format("delta") \
        .outputMode("append") \
        .trigger(processingTime="%d seconds" % trigger_interval_secs) \
        .option("checkpointLocation", checkpoint_path) \
        .start(path=output_path, queryName='SS-rate-datasource-delta-lake-sink-experiment')

    query.awaitTermination()

    print("Query terminated.")
    exc = query.exception()
    if exc:
        print("Exception: %s\n%s" % (exc.desc, exc.stackTrace))

    print("Done...")
