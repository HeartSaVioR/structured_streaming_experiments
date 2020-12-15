# -*- coding: utf-8 -*-

from datasources.BaseDataSource import BaseDataSource
from datasources.UnsupportedException import UnsupportedException

# TODO: can we do this dynamically? leverage __init__?
from datasources.file import FileDataSource
from datasources.iceberg import IcebergDataSource
from datasources.kafka import KafkaDataSource
from datasources.rate import RateDataSource


def lookup_data_source(data_source_format):
    sub_classes = BaseDataSource.__subclasses__()
    sub_class = next((x for x in sub_classes if x.format().upper() == data_source_format.upper()), None)
    if not sub_class:
        raise UnsupportedException("Data source %s is not supported" % data_source_format)
    return sub_class()
