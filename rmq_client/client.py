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
        """
        Initializes the RMQClient by initializing its member variables.
        """
        print("client __init__")
        self._consumer = RMQConsumer()
        self._producer = RMQProducer()

    def start(self):
        """
        Starts the RMQClient by starting its subsequent consumer and producer
        instances.
        """
        print("client start()")
        self._consumer.start()
        self._producer.start()

    def subscribe(self, topic, callback):
        """
        Subscribes to the input topic. A message received for the given topic
        will result in the given callback being called.

            callback(message: bytes)

        The function returns immediately but will dispatch the subscription job
        to another process, meaning that the actual subscribing will take place
        shortly after the function returns.

        :param str topic: topic to subscribe to
        :param callable callback: callback on message received
        """
        print("client subscribe()")
        # dynamically add a subscription to a topic
        self._consumer.subscribe(topic, callback)

    def publish(self, topic, message):
        """
        Publishes a message on the given topic. The publising work is dispatched
        to another process, meaning publishing probably has not completed by the
        time this function returns.

        :param str topic: topic to publish on
        :param str message: message to publish
        """
        print("client publish()")
        # publishes a message to the provided topic
        self._producer.publish(topic, message)
