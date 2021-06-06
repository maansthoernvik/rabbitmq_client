import signal
import logging
import threading
import sys

from pika.exchange_type import ExchangeType

from rabbitmq_client import RMQConsumer, ConsumeOK
from rabbitmq_client import ConsumeParams, QueueParams, ExchangeParams


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


def on_msg(message):
    if isinstance(message, ConsumeOK):
        print("GOT CONSUME OK")
    else:
        print(f"GOT MESSAGE: {message}")


consumer.consume(
    ConsumeParams(on_msg),
    exchange_params=ExchangeParams("direct",
                                   exchange_type=ExchangeType.fanout),
    routing_key="rkey"
)

threading.Event().wait()
