import signal
import logging
import threading

import sys

from rabbitmq_client.new_consumer import RMQConsumer


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

threading.Event().wait()
