# -*- coding: utf-8 -*-

import sys
from time import sleep, time
from src.datasources.kafka import KafkaProducer
from random import randint

TEST_DATA_FILE_PATH = "resources/test_data.txt"


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("USAGE: %s [kafka_bootstrap_servers] [topic] [records per second]" % sys.argv[0])
        sys.exit(1)

    kafka_bootstrap_servers = sys.argv[1]
    topic = sys.argv[2]
    records_per_second = int(sys.argv[3])

    print("=" * 40)
    print("Bootstrap servers: %s" % kafka_bootstrap_servers)
    print("Topic: %s" % topic)
    print("Records per second: %s" % records_per_second)
    print("=" * 40)

    with open(TEST_DATA_FILE_PATH, "r") as fr:
        data_lines = map(lambda x: x.strip(), fr.readlines())
        producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers, value_serializer=str.encode)

        while True:
            start_time = time()
            for _ in range(records_per_second):
                producer.send(topic, data_lines[randint(0, len(data_lines) - 1)])
            end_time = time()

            sleep(1 - (end_time - start_time) / 1000)
