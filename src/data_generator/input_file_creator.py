# -*- coding: utf-8 -*-

import sys
import os
from time import sleep
from tempfile import mkdtemp
from uuid import uuid1
from datetime import datetime
from random import randint

TEST_DATA_FILE_PATH = "resources/test_data.txt"


def create_file(input_dir_path, dir_pattern, temp_dir_path, num_of_lines, data_lines):
    file_name = str(uuid1()) + ".txt"
    temp_file_path = temp_dir_path + "/" + file_name
    with open(temp_file_path, "w") as fw:
        for idx in range(num_of_lines):
            fw.write(data_lines[randint(0, len(data_lines) - 1)] + "\n")
    cur_datetime = datetime.utcnow()
    target_dir = input_dir_path + "/" + cur_datetime.strftime(dir_pattern)
    if not os.access(target_dir, os.R_OK):
        print("Input directory [%s] doesn't exist - creating one..." % target_dir)
        os.makedirs(target_dir)
    target_file_path = target_dir + "/" + file_name
    os.rename(temp_file_path, target_file_path)


def main():
    if len(sys.argv) < 5:
        print("USAGE: %s [input_dir_path][dir_pattern (follow the format of datetime)]"
              "[seconds per file][lines per file]" % sys.argv[0])
        sys.exit(1)

    input_dir_path = sys.argv[1]
    dir_pattern = sys.argv[2]
    seconds_per_file = float(sys.argv[3])
    lines_per_file = int(sys.argv[4])
    temp_dir_path = mkdtemp()

    print("=" * 40)
    print("Input directory path: %s" % input_dir_path)
    print("Seconds per file: %s" % seconds_per_file)
    print("Number of lines per file: %s" % lines_per_file)
    print("=" * 40)

    with open(TEST_DATA_FILE_PATH, "r") as fr:
        data_lines = [x.strip() for x in fr.readlines()]

        while True:
            create_file(input_dir_path, dir_pattern, temp_dir_path, lines_per_file, data_lines)
            sleep(seconds_per_file)


if __name__ == "__main__":
    main()
