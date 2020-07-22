# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

import sys

if __name__ == "__main__":
    if len(sys.argv) < 8:
        print("USAGE: %s [input_path] [max_files_in_batch] [iceberg_warehouse_path] [table_name] [checkpoint_path] [trigger_interval_secs] [output_partition_num]" % sys.argv[0])
        sys.exit(1)

    input_path = sys.argv[1]
    max_files_in_batch = int(sys.argv[2])
    iceberg_warehouse_path = sys.argv[3]
    table_name = sys.argv[4]
    checkpoint_path = sys.argv[5]
    trigger_interval_secs = int(sys.argv[6])
    output_partition_num = int(sys.argv[7])

    print("=" * 40)
    print("Input path: %s" % input_path)
    print("Max files in batch: %s" % max_files_in_batch)
    print("Iceberg warehouse path: %s" % iceberg_warehouse_path)
    print("Iceberg table name: %s" % table_name)
    print("Checkpoint path: %s" % checkpoint_path)
    print("Trigger interval: %s" % trigger_interval_secs)
    print("Number of output partitions: %s" % output_partition_num)
    print("=" * 40)

    spark = SparkSession \
        .builder \
        .appName("SSFileDataSourceWithIcebergSink") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", iceberg_warehouse_path) \
        .getOrCreate()

    spark.sql("""
        CREATE TABLE IF NOT EXISTS {} (
            value string COMMENT 'content'
        )
        USING iceberg
        """.format(table_name))

    df = spark \
        .readStream \
        .format("text") \
        .option("path", input_path) \
        .option("maxFilesPerTrigger", 100) \
        .load()

    if output_partition_num > 0:
        df = df.repartition(output_partition_num)

    query = df \
        .writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="%d seconds" % trigger_interval_secs) \
        .option("path", iceberg_warehouse_path + "/default/" + table_name) \
        .option("checkpointLocation", checkpoint_path) \
        .start(queryName='SS-file-datasource-iceberg-sink-experiment')

    query.awaitTermination()

    print("Query terminated.")
    exc = query.exception()
    if exc:
        print("Exception: %s\n%s" % (exc.desc, exc.stackTrace))

    print("Done...")
