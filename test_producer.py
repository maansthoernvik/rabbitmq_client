import signal
import logging
import threading
import sys

from pika.exchange_type import ExchangeType

from rabbitmq_client import RMQProducer, QueueParams, ExchangeParams


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
producer.publish(b"queue publish 2", queue_params=QueueParams("queue"))
producer.publish(b"queue publish 3", queue_params=QueueParams("queue"))
producer.publish(b"exchange publish 1",
                 exchange_params=ExchangeParams("direct"),
                 routing_key="rkey")
producer.publish(
    b"exchange publish 2",
    exchange_params=ExchangeParams("fanout",
                                   exchange_type=ExchangeType.fanout))


threading.Event().wait()
