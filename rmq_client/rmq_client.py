from multiprocessing import Event

from .producer import Producer
from .consumer import Consumer
from .rmq_connection import RMQConnection, get_connection_count


class NoConsumerError(Exception):
    pass


class NoProducerError(Exception):
    pass


class RMQClient:

    _consumers = []

    _consumer_connection: RMQConnection = None
    _producer_connection: RMQConnection = None

    def __init__(self, consumer=True, producer=True):
        """
        Constructor for the RMQClient.
        """
        if consumer:
            self._consumer_connection = RMQConnection()

        if producer:
            self._producer_connection = RMQConnection()

    def start(self):
        print("Client starting consumer")
        if self._consumer_connection:
            self._consumer_connection.start()

        print("Client starting producer")
        if self._producer_connection:
            self._producer_connection.start()

    def stop(self):
        print("Stopping client")

        print("Stopping consumer")
        self._consumer_connection.close_connection()
        print("Stopping producer")
        self._producer_connection.close_connection()

        print("Stopping consumer IO loop")
        self._consumer_connection.stop()
        print("Stopping producer IO loop")
        self._producer_connection.stop()

    def create_consumer(self):
        consumer = Consumer(self._consumer_connection)
        print("Number of connections: {}".format(get_connection_count()))
        consumer.create_channel()
