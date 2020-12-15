# -*- coding: utf-8 -*-

def init_kafka_source(spark_session, options):
    bootstrap_servers = options["source-kafka-option-bootstrap-servers"]
    subscribe = options["source-kafka-option-subscribe"]
    starting_offsets = options["source-kafka-option-starting-offsets"]
    fail_on_data_loss = options["source-kafka-option-fail-on-data-loss"]
    # TODO: more options?

    return spark_session \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", subscribe) \
        .option("startingOffsets", starting_offsets) \
        .option("failOnDataLoss", fail_on_data_loss) \
        .load()
