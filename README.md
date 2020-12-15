# Structured Streaming Experiments

This repository contains some implementations of streaming queries to experiment.

# How to run?

Prerequisite: `python`, `pip` and `virtualenv` should be installed in prior.

```
make clean
make all
```

Once the commands run successfully, you will get three files in `dist/`,

- libs.zip
- jobs.zip
- main.py

(TODO: improve Makefile to provide a command to update only jobs.zip)

libs.zip and jobs.zip will be provided as dependencies (via `--py-files`). `main.py` will be provided as an entry point.

For example, suppose you'd like to execute streaming query via simple query executor, you'll want to run it like below:

```
<SPARK_HOME>/bin/spark-submit --master "local[*]" --py-files ./dist/jobs.zip,./dist/libs.zip \
./dist/main.py simple_query <options for simple query>
```

## Simple query executor

Simple query executor lets you to construct the query with pre-configured source and sink. Given it's pre-configured,
the functionality is merely like reading from source and directly writing to sink without transformation (does some
transformation if sink requires).

For example, suppose you don't have any data to read but want to start with any generated data to experiment the basic
functionality of sink. In this case, you can use 'rate' data source to generate the data, and write to the available sink.
Suppose you're writing the date to the Kafka topic, then the command to run the query will follow:

```
<SPARK_HOME>/bin/spark-submit --master "local[*]" --py-files ./dist/jobs.zip,./dist/libs.zip \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 \
./dist/main.py simple_query --source-format=rate --sink-format=kafka \
--source-option-rows-per-second=100 \
--sink-option-bootstrap-servers=localhost:9092 \
--sink-option-topic="output-topic" \
--checkpoint-location=/tmp/checkpoint-rate-to-kafka
```

(As you see, some data sources require you to provide dependencies in Spark classpath.)

`--source-format` defines the data source to use for source, whereas `--sink-format` defines the data source to use for sink.

You can provide "help" to the first option to print out all available data sources and options (common, and data source related).

```
<SPARK_HOME>/bin/spark-submit --master "local[*]" --py-files ./dist/jobs.zip,./dist/libs.zip \
./dist/main.py simple_query help
```

(You would wonder why it has to go through `spark-submit`. You don't necessarily need to do that, but then you'll need
to deal with dependencies. If you have configured `virtualenv` (or installed all dependencies via `pip`) for the code,
simply including `./src` to the PYTHONPATH would probably work.)

## Data generator

To support testing long-running query, there're a couple of data generators available.
Data generator doesn't follow the options above, and doesn't require PySpark to work with.

* Input creator for file: src/data_generator/input_file_creator.py

```
USAGE: %s [input_dir_path] [dir_pattern (follow the format of datetime)] [seconds per file] [lines per file]
```

* Input creator for Kafka: src/data_generator/kafka_input_record_creator.py

```
USAGE: %s [kafka_bootstrap_servers] [topic] [records per second]
```
