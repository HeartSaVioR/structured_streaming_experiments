# -*- coding: utf-8 -*-

from pyspark.sql.types import StructType

from datasources.BaseDataSource import BaseDataSource

from textwrap import dedent
from json import loads


class FileDataSource(BaseDataSource):
    @classmethod
    def format(cls):
        return "file"

    def init_source(self, spark_session, options):
        source_format = options.get("format", "text")
        schema = options["schema"]
        input_path = options["path"]
        max_files_in_batch = options["max-files-per-trigger"]

        print(schema)
        schema_type = StructType.fromJson(loads(schema))

        return spark_session \
            .readStream \
            .format(source_format) \
            .schema(schema_type) \
            .option("path", input_path) \
            .option("maxFilesPerTrigger", max_files_in_batch) \
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
        schema (required, json format)
        max-files-per-trigger (required)
        format (option, default value: text)
        
        SINK
        ====
        path (required)
        format (option, default value: text)
        '''
        return dedent(usage)
