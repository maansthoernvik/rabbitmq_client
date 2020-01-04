from multiprocessing import Queue as IPCQueue, Process

from .producer_connection import create_producer_connection


class RMQProducer:

    _connection_process: Process

    _work_queue: IPCQueue

    def __init__(self):
        """
        Initializes the RMQProducer.
        """
        print("producer __init__")
        self._work_queue = IPCQueue()

    def start(self):
        """
        Starts the producer's connection process in order to be able to publish
        messages.
        """
        print("producer start()")
        self._connection_process = Process(target=create_producer_connection,
                                           args=(self._work_queue,))
        self._connection_process.start()

    def publish(self, exchange, routing_key):
        """
        Publishes a message on the supplied exchange with the provided
        routing_key.

        :param exchange:
        :param routing_key:
        """
        pass
