from multiprocessing import Queue as IPCQueue, Process

from .defs import Publish
from .producer_connection import create_producer_connection


class RMQProducer:
    """
    Class RMQProducer

    Interface for producer-related operations towards a RabbitMQ server.
    """

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
        messages. The connection process is maintained in another process, the
        work queue passed along to the new process is process shared to allow
        for the controlling process to issue commands to the connection process,
        for example to publish messages.
        """
        print("producer start()")
        self._connection_process = Process(target=create_producer_connection,
                                           args=(self._work_queue,))
        self._connection_process.start()

    def publish(self, topic, message):
        """
        Publishes a message on the supplied topic.

        :param str topic: topic to publish on
        :param str message: message content
        """
        print("producer publish()")
        self._work_queue.put(Publish(message, topic=topic))
