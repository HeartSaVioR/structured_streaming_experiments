# -*- coding: utf-8 -*-

from datasources.BaseDataSource import BaseDataSource

from textwrap import dedent


class ConsoleDataSource(BaseDataSource):
    @classmethod
    def format(cls):
        return "console"

    def init_sink(self, data_frame, options):
        return data_frame \
            .writeStream \
            .format("console")

    def usage(self):
        usage = '''
        SOURCE
        ======
        Unavailable
        
        SINK
        ====
        '''
        return dedent(usage)
