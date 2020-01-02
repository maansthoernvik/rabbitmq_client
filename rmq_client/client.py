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

    def stop(self):
        print("client stop()")
        self._consumer.stop()
        self._producer.stop()

    def subscribe(self):
        # dynamically add a subscription to a topic
        pass

    def publish(self):
        # publishes a message to the provided topic
        pass
