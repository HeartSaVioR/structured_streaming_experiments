# -*- coding: utf-8 -*-

from datasources.BaseDataSource import BaseDataSource

from textwrap import dedent


class RateDataSource(BaseDataSource):
    @classmethod
    def format(cls):
        return "rate"

    def init_source(self, spark_session, options):
        input_records_per_second = int(options["rows-per-second"])

        return spark_session \
            .readStream \
            .format("rate") \
            .option("rowsPerSecond", input_records_per_second) \
            .load()

    def usage(self):
        usage = '''
        SOURCE
        ======
        rows-per-second (required)
        
        SINK
        ====
        Unavailable
        '''
        return dedent(usage)
