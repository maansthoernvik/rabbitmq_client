import pika

import definitions as defs

from .rmq_connection import RMQConnection


class Consumer:

    _connection: RMQConnection
    _channel: pika.spec.Channel

    topic = None
    queue = None
    routing_key = None
    callback = None

    def __init__(self, connection):
        self._connection = connection

    def create_channel(self):
        self._channel = self._connection.open_channel()
        print("Opened channel: {}".format(self._channel))

    def subscribe(self, topic, routing_key, callback):
        print(topic)
        print(routing_key)
        print(callback)

        self.topic = topic
        self.routing_key = routing_key
        self.callback = callback

        self.declare_exchange(topic, defs.EXCHANGE_TYPE_TOPIC)
        self.declare_queue()
        self.bind_queue()
        self.start_consuming(topic, callback)

    def declare_exchange(self, topic, exchange_type):
        print(topic)
        print(exchange_type)

        if exchange_type == defs.EXCHANGE_TYPE_TOPIC:
            self._channel.exchange_declare(
                topic, type=exchange_type, callback=self.on_exchange_declared,
                auto_delete=True
            )

    def on_exchange_declared(self, frame):
        print(frame)

        self._channel.queue_declare(
            defs.AUTO_CREATE_QUEUE_NAME, auto_delete=True, exclusive=True,
            callback=self.on_queue_declared
        )

    def on_queue_declared(self, frame):
        print(frame)
        self.queue = frame.method.queue

        self._channel.queue_bind(
            frame.method.queue, self.topic, self.routing_key,
            callback=self.on_queue_bound
        )

    def on_queue_bound(self, frame):
        print(frame)

        self._channel.basic_consume(self.queue, self.callback)
