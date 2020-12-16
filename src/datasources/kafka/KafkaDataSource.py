# -*- coding: utf-8 -*-

from datasources.BaseDataSource import BaseDataSource

from textwrap import dedent


class KafkaDataSource(BaseDataSource):
    @classmethod
    def format(cls):
        return "kafka"

    def init_source(self, spark_session, options):
        bootstrap_servers = options["bootstrap-servers"]
        subscribe = options["subscribe"]
        starting_offsets = options["starting-offsets"]
        fail_on_data_loss = options.get("fail-on-data-loss", "true")

        return spark_session \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", subscribe) \
            .option("startingOffsets", starting_offsets) \
            .option("failOnDataLoss", fail_on_data_loss) \
            .load()

    def init_sink(self, data_frame, options):
        bootstrap_servers = options["bootstrap-servers"]
        topic = options["topic"]

        return data_frame \
            .selectExpr("to_json(struct(*)) as value") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("topic", topic)

    def usage(self):
        usage = '''
        SOURCE
        ======
        bootstrap-servers (required)
        subscribe (required)
        starting-offsets (required)
        fail-on-data-loss (optional, default value: true)
        
        SINK
        ====
        bootstrap-servers (required)
        topic (required)
        '''
        return dedent(usage)
