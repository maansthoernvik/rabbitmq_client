from .consumer import RMQConsumer
from .producer import RMQProducer


class RMQClient:
    """
    Class RMQClient

    Handles connection to RabbitMQ and provides and interface for applications
    wanting to either publish
    """

    _consumer: RMQConsumer
    _producer: RMQProducer

    def __init__(self):
        print("client __init__")
        self._consumer = RMQConsumer()
        self._producer = RMQProducer()

    def start(self):
        print("client start()")
        self._consumer.start()
        self._producer.start()

    def subscribe(self, topic, callback):
        print("client subscribe()")
        # dynamically add a subscription to a topic
        self._consumer.subscribe(topic, callback)

    def publish(self, topic, message):
        print("client publish()")
        # publishes a message to the provided topic
        self._producer.publish(topic, message)
