# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

import sys

from datasources import lookup_data_source


def read_args(args):
    args_with_long_option = [x for x in args if x.startswith("--")]
    args_remove_long_option = [x[len("--"):] for x in args_with_long_option]
    options_list = [(x.split("=")[0], "=".join(x.split("=")[1:])) for x in args_remove_long_option]
    return {k: v for k, v in options_list}


def extract_options(options, prefix):
    return {k[len(prefix):]: v for k, v in options.items() if k.startswith(prefix)}


def print_usage_data_source(data_source_instance):
    print("Usage of data source %s" % data_source_instance.format())
    print("=" * 50)
    print(data_source_instance.usage())
    print("=" * 50)


def main(args):
    options = read_args(args)

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

    source_instance = lookup_data_source(source)
    sink_instance = lookup_data_source(sink)

    print_usage_data_source(source_instance)
    print_usage_data_source(sink_instance)

    source_options = extract_options(options, "source-option-")
    sink_options = extract_options(options, "sink-option-")

    data_frame = source_instance.init_source(spark, source_options)
    writer_builder = sink_instance.init_sink(data_frame, sink_options)

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
