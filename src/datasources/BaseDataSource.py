# -*- coding: utf-8 -*-

from datasources.UnsupportedException import UnsupportedException


class BaseDataSource(object):
    @classmethod
    def format(cls):
        raise NotImplementedError("format should be implemented")

    def init_source(self, spark_session, options):
        raise UnsupportedException("Source not supported")

    def init_sink(self, data_frame, options):
        raise UnsupportedException("Sink not supported")

    def usage(self):
        raise NotImplementedError("Usage should be implemented")
