import signal
import logging
import threading
import time

import sys

from rabbitmq_client import RMQConsumer


logger = logging.getLogger("rabbitmq_client")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
logger.addHandler(handler)


producer = RMQConsumer()
producer.start()


def stop(*args):
    producer.stop()
    sys.exit(0)


signal.signal(signal.SIGINT, stop)

time.sleep(1)
producer._channel.basic_publish(exchange="", routing_key="queue", body=b"poo!")

threading.Event().wait()
