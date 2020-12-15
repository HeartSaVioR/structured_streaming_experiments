# -*- coding: utf-8 -*-

def init_rate_source(spark_session, options):
    input_records_per_second = int(options["source-rate-option-rows-per-second"])
    # TODO: more options?

    return spark_session \
        .readStream \
        .format("rate") \
        .option("rowsPerSecond", input_records_per_second) \
        .load()
