# Structured Streaming Experiments

This repository contains some implementations of streaming queries to experiment.

## File Data Source stress test

This experiment runs a streaming query which leverages file stream source and file stream sink,
to observe issues on various input/output workloads, like huge number of input files,
huge number of output files per batch, etc.

* Source file: src/file_data_source.py
* Input file creator source file: src/input_file_creator.py

## Kafka Data Source test

This experiment runs a streaming query which leverages kafka source and kafka sink, to test
various features in offsets as well as observe issues on long run query.

* Source file: src/kafka_data_source.py
* Input file creator source file: src/kafka_input_record_creator.py

