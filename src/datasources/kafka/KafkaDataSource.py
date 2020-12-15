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
        fail_on_data_loss = options["fail-on-data-loss"]

        return spark_session \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", subscribe) \
            .option("startingOffsets", starting_offsets) \
            .option("failOnDataLoss", fail_on_data_loss) \
            .load()

    def init_sink(self, data_frame, options):
        sink_format = options.get("format", "text")
        output_path = options["path"]

        return data_frame \
            .writeStream \
            .format(sink_format) \
            .option("path", output_path)

    def usage(self):
        usage = '''
        SOURCE
        ======
        path (required)
        max-files-per-trigger (require)
        format (option, default value: text)
        
        SINK
        ====
        path (required)
        format (option, default value: text)
        '''
        return dedent(usage)
