# -*- coding: utf-8 -*-

def init_file_sink(data_frame, options):
    output_path = options["sink-file-option-path"]
    # TODO: more options?

    return data_frame \
        .writeStream \
        .format("text") \
        .option("path", output_path)
