from kafka import KafkaConsumer
import sys
from multiprocessing import Pool

TOPIC = 'bwpkpr'
GROUP_ID = 'D'
BOOTSTRAP_SERVERS = ['back1145.back:9092',
                     'back1146.back:9092',
                     'back1151.back:9092',
                     'back1152.back:9092',
                     'back1153.back:9092',
                     'back1154.back:9092',
                     'back1155.back:9092'
                     ]

AUTO_OFFSET_RESET = 'earliest'
ENABLE_AUTO_COMMIT = True
CONSUMER_TIMETOUT_MS = 100000


def run(process_id):
    consumer = KafkaConsumer(TOPIC,
                             group_id=GROUP_ID,
                             auto_offset_reset=AUTO_OFFSET_RESET,
                             enable_auto_commit=ENABLE_AUTO_COMMIT,
                             consumer_timeout_ms=CONSUMER_TIMETOUT_MS,
                             bootstrap_servers=BOOTSTRAP_SERVERS)

    with open('process_' + str(process_id) + '.out', 'w') as outfile:
        for msg in consumer:
            s = 'process_id: {process_id} topic: {topic}, partition: {partition} offset: {offset}\n'.format(
                process_id=process_id,
                topic=msg.topic,
                partition=msg.partition,
                offset=msg.offset)
            outfile.write(s)


if __name__ == '__main__':
    number_process = int(sys.argv[1])
    with Pool(number_process) as pool:
        pool.map(run, range(number_process))
