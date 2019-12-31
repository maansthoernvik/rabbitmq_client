from kombu import Connection

from subscriber import Subscriber
from publisher import Publisher


class RMQClient:

    subscriber = None
    publisher = None

    def __init__(self, connection_string):
        self._connection_string = connection_string
        self.subscriber = Subscriber(Connection(self._connection_string))
        self.publisher = Publisher(self._connection_string)

    def publish(self, topic, routing_key, message):
        self.publisher.publish(topic, routing_key, message)

    def subscribe(self, topic, routing_key, callback):
        self.subscriber.subscribe(topic, routing_key, callback)

    def start_consuming(self):
        self.subscriber.start_consuming()
