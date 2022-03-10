import signal
import logging
import threading
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

producer.publish(b"queue publish 1", queue_params=QueueParams("queue"))

threading.Event().wait()
