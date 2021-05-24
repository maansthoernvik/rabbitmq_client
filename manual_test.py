import signal
import logging
import threading

import sys

from rabbitmq_client import RMQConsumer
from rabbitmq_client import ConsumeParams, QueueParams


logger = logging.getLogger("rabbitmq_client")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
logger.addHandler(handler)


consumer = RMQConsumer()
consumer.start()


def stop(*args):
    consumer.stop()
    sys.exit(0)


signal.signal(signal.SIGINT, stop)

consumer.consume(
    ConsumeParams(lambda x: print(x)),
    queue_params=QueueParams("queue")
)

threading.Event().wait()
