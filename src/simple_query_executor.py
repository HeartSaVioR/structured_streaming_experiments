# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

import sys

from src.datasources.file.file_sink import init_file_sink
from src.datasources.file.file_source import init_file_source
from src.datasources.iceberg import init_iceberg_sink
from src.datasources.kafka import init_kafka_sink
from src.datasources.kafka.kafka_source import init_kafka_source
from src.datasources.rate import init_rate_source

SOURCE_FUNCTION_MAP = {
    "file": init_file_source,
    "kafka": init_kafka_source,
    "rate": init_rate_source
}

SINK_FUNCTION_MAP = {
    "file": init_file_sink,
    "kafka": init_kafka_sink,
    "iceberg": init_iceberg_sink
}


def lookup_source_function(source):
    try:
        return SOURCE_FUNCTION_MAP[source]
    except KeyError:
        print("Error: %s unsupported in source" % source, file=sys.stderr)
        exit(2)


def lookup_sink_function(sink):
    try:
        return SINK_FUNCTION_MAP[sink]
    except KeyError:
        print("Error: %s unsupported in sink" % sink, file=sys.stderr)
        exit(2)


def read_args():
    args = sys.argv
    args_with_long_option = [x for x in args if x.startswith("--")]
    args_remove_long_option = [x[len("--"):] for x in args_with_long_option]
    options_list = [tuple(x.split("=")) for x in args_remove_long_option]
    return {k: v for k, v in options_list}


def main():
    options = read_args()

    print("=" * 50)
    print("Options provided:")
    for key, value in options.items():
        print("%s: %s" % (key, value))
    print("=" * 50)

    source = options["source-format"]
    sink = options["sink-format"]

    print("source: %s" % source)
    print("sink: %s" % sink)

    app_name = "SS%sTo%s" % (source.capitalize(), sink.capitalize())

    spark = SparkSession \
        .builder \
        .appName(app_name) \
        .getOrCreate()

    source_init_fn = lookup_source_function(source)
    sink_init_fn = lookup_sink_function(sink)

    data_frame = source_init_fn(spark, options)
    writer_builder = sink_init_fn(data_frame, options)

    output_mode = options.get("output-mode", "append")
    trigger_interval_secs = options.get("trigger-interval-secs", 0)
    checkpoint_location = options["checkpoint-location"]

    query = writer_builder \
        .outputMode(output_mode) \
        .trigger(processingTime="%d seconds" % trigger_interval_secs) \
        .option("checkpointLocation", checkpoint_location) \
        .start()

    query.awaitTermination()

    print("Query terminated.")
    exc = query.exception()
    if exc:
        print("Exception: %s\n%s" % (exc.desc, exc.stackTrace))

    print("Done...")


if __name__ == "__main__":
    main()
