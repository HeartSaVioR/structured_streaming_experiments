# -*- coding: utf-8 -*-

def init_file_source(spark_session, options):
    input_path = options["source-file-option-path"]
    max_files_in_batch = options["source-file-option-max-files-per-trigger"]
    # TODO: more options?

    return spark_session \
        .readStream \
        .format("text") \
        .option("path", input_path) \
        .option("maxFilesPerTrigger", max_files_in_batch) \
        .load()
