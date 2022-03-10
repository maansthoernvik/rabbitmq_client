import signal
import logging
import threading
import sys

from rabbitmq_client import RMQConsumer, ConsumeOK, QueueParams
from rabbitmq_client import ConsumeParams


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


def on_msg(message, ack=None):
    if isinstance(message, ConsumeOK):
        print("GOT CONSUME OK")
    else:
        print(f"GOT MESSAGE: {message}")
        ack()


consumer.consume(
    ConsumeParams(on_msg),
    queue_params=QueueParams("queue")
)

threading.Event().wait()
