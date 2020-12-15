# -*- coding: utf-8 -*-

from datasources.BaseDataSource import BaseDataSource

from textwrap import dedent


class IcebergDataSource(BaseDataSource):
    @classmethod
    def format(cls):
        return "iceberg"

    def init_sink(self, data_frame, options):
        # for hadoop catalog table, provide full path
        # for hive catalog table backed by HMS, use table name
        table_name = options["table-name"]

        return data_frame \
            .writeStream \
            .format("iceberg") \
            .option("path", table_name)

    def usage(self):
        usage = '''
        SOURCE
        ======
        Unavailable
        
        SINK
        ====
        table-name (required)
        '''
        return dedent(usage)
