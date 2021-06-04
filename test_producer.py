import signal
import logging
import threading
import time

import sys

from rabbitmq_client import RMQProducer, QueueParams


logger = logging.getLogger("rabbitmq_client")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
logger.addHandler(handler)


producer = RMQProducer()
producer.start()


def stop(*args):
    producer.stop()
    sys.exit(0)


signal.signal(signal.SIGINT, stop)

time.sleep(0.5)
producer.publish(b"body", queue_params=QueueParams("queue"))

threading.Event().wait()
