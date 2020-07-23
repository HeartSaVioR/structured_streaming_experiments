# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

import sys

if __name__ == "__main__":
    if len(sys.argv) < 6:
        print("USAGE: %s [input_records_per_second] [iceberg_warehouse_path] [table_name] " +
              "[checkpoint_path] [trigger_interval_secs] [output_partition_num]")
        sys.exit(1)

    input_records_per_second = sys.argv[1]

    iceberg_warehouse_path = sys.argv[2]
    table_name = sys.argv[3]

    checkpoint_path = sys.argv[4]
    trigger_interval_secs = int(sys.argv[5])
    output_partition_num = int(sys.argv[6])

    print("=" * 40)
    print("Input records per second: %s" % input_records_per_second)
    print("Iceberg warehouse path: %s" % iceberg_warehouse_path)
    print("Iceberg table name: %s" % table_name)
    print("Checkpoint path: %s" % checkpoint_path)
    print("Trigger interval: %s" % trigger_interval_secs)
    print("Number of output partitions: %s" % output_partition_num)
    print("=" * 40)

    spark = SparkSession \
        .builder \
        .appName("SSRateDataSourceToIceberg") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", iceberg_warehouse_path) \
        .getOrCreate()

    spark.sql("""
        CREATE TABLE IF NOT EXISTS {} (
            `timestamp` timestamp COMMENT 'timestamp',
            `value` long COMMENT 'content'
        )
        USING iceberg
        """.format(table_name))

    df = spark \
        .readStream \
        .format("rate") \
        .option("rowsPerSecond", input_records_per_second) \
        .load()

    if output_partition_num > 0:
        df = df.repartition(output_partition_num)

    query = df \
        .selectExpr("value", "timestamp") \
        .writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="%d seconds" % trigger_interval_secs) \
        .option("path", iceberg_warehouse_path + "/default/" + table_name) \
        .option("checkpointLocation", checkpoint_path) \
        .start(queryName='SS-rate-datasource-iceberg-sink-experiment')

    query.awaitTermination()

    print("Query terminated.")
    exc = query.exception()
    if exc:
        print("Exception: %s\n%s" % (exc.desc, exc.stackTrace))

    print("Done...")
