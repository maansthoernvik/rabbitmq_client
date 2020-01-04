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

    def start(self):
        print("client start()")
        self._consumer.start()

    def subscribe(self):
        print("client subscribe()")
        # dynamically add a subscription to a topic
        pass

    def publish(self):
        print("client publish()")
        # publishes a message to the provided topic
        pass
