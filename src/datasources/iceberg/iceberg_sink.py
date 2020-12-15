# -*- coding: utf-8 -*-

def init_iceberg_sink(data_frame, options):
    # for hadoop catalog table, provide full path
    # for hive catalog table backed by HMS, use table name
    table_name = options["sink-iceberg-table-name"]
    # TODO: more options?

    return data_frame \
        .writeStream \
        .format("iceberg") \
        .option("path", table_name)
