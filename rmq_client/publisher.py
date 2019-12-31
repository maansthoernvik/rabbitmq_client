from kombu import Connection, Exchange
from kombu.pools import producers


class Publisher:

    def __init__(self, connection_string):
        self._connection_string = connection_string

    def publish(self, topic, routing_key, message):
        with Connection(self._connection_string) as conn:
            with producers[conn].acquire(block=True) as prod:
                exchange = Exchange(topic, type="topic")
                prod.publish(message,
                             exchange=exchange,
                             routing_key=routing_key)
