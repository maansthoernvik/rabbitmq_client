import logging
from multiprocessing import Queue as IPCQueue, Process

from .log import LogItem
from .producer_connection import create_producer_connection
from .producer_defs import *


class RMQProducer:
    """
    Class RMQProducer

    Interface for producer-related operations towards a RabbitMQ server.
    """
    # general
    _log_queue: IPCQueue

    # IPC
    _connection_process: Process

    _work_queue: IPCQueue

    def __init__(self, log_queue):
        """
        Initializes the RMQProducer.
        """
        self._log_queue = log_queue

        self._work_queue = IPCQueue()

        self._log_queue.put(
            LogItem("__init__", RMQProducer.__name__, level=logging.DEBUG)
        )

    def start(self):
        """
        Starts the producer's connection process in order to be able to publish
        messages. The connection process is maintained in another process, the
        work queue passed along to the new process is process shared to allow
        for the controlling process to issue commands to the connection process,
        for example to publish messages.
        """
        self._log_queue.put(
            LogItem("start", RMQProducer.__name__)
        )
        self._connection_process = Process(target=create_producer_connection,
                                           args=(self._work_queue,
                                                 self._log_queue))
        self._connection_process.start()

    def stop(self):
        """
        Stops the RMQConsumer, tearing down the RMQProducerConnection process.
        """
        self._log_queue.put(
            LogItem("stop", RMQProducer.__name__)
        )
        self._connection_process.terminate()

    def publish(self, topic, message):
        """
        Publishes a message on the supplied topic.

        :param str topic: topic to publish on
        :param str message: message content
        """
        self._log_queue.put(
            LogItem("publish", RMQProducer.__name__, level=logging.DEBUG)
        )
        self._work_queue.put(Publish(topic, message))

    def rpc_request(self, receiver, message, correlation_id, reply_to):
        self._log_queue.put(
            LogItem("rpc_request", RMQProducer.__name__, level=logging.DEBUG)
        )
        self._work_queue.put(RPCRequest(receiver,
                                        message,
                                        correlation_id,
                                        reply_to))

    def rpc_reply(self, receiver, message, correlation_id):
        self._log_queue.put(
            LogItem("rpc_reply", RMQProducer.__name__, level=logging.DEBUG)
        )
        self._work_queue.put(RPCResponse(receiver,
                                         message,
                                         correlation_id))

