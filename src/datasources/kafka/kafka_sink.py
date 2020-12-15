# -*- coding: utf-8 -*-

def init_kafka_sink(data_frame, options):
    bootstrap_servers = options["sink-kafka-option-bootstrap-servers"]
    topic = options["sink-kafka-option-topic"]
    # TODO: more options?

    return data_frame \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("topic", topic)
