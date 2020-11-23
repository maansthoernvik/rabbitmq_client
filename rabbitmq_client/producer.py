import logging

from multiprocessing import Queue as IPCQueue, Process

from rabbitmq_client import log

from .log import LogManager
from .producer_connection import create_producer_connection
from .producer_defs import *


LOGGER = logging.getLogger(__name__)


class RMQProducer:
    """
    Interface for producer-related operations towards a RabbitMQ server.

    =================
    Pub/Sub
    =================
    Supported, not much to say really. Will publish to named exchange and use
    'fanout' to distribute messages to all listening queues.

    =================
    RPC
    =================
    Acts more as an interface towards the producer connection for the
    RPCHandler than actually doing something useful. Only instantiates work
    items and puts them on the work queue.
    """

    def __init__(self,
                 connection_parameters):
        """
        :param connection_parameters: pika.ConnectionParameters or None
        """
        LOGGER.debug("__init__")

        self._work_queue = IPCQueue()

        log_manager: LogManager = log.get_log_manager()

        self._connection_process = Process(
            target=create_producer_connection,
            args=(
                connection_parameters,
                self._work_queue,

                # This is fine since we're still in the same process!
                log_manager.log_queue,
                log_manager.log_level
            )
        )

    def start(self):
        """
        Starts the producer's connection process in order to be able to publish
        messages. The connection process is maintained in another process, the
        work queue passed along to the new process is process shared to allow
        for the controlling process to issue commands to the connection
        process, for example to publish messages.
        """
        LOGGER.info("start")

        self._connection_process.start()

    def stop(self):
        """
        Stops the RMQConsumer, tearing down the RMQProducerConnection process.
        """
        LOGGER.info("stop")

        self.flush_and_close_queues()

        self._connection_process.terminate()
        self._connection_process.join(timeout=2)

    def flush_and_close_queues(self):
        """
        Flushed process shared queues in an attempt to stop background threads.
        """
        while not self._work_queue.empty():
            self._work_queue.get()
        self._work_queue.close()
        # In order for client.stop() to be reliable and consistent, ensure
        # thread stop.
        self._work_queue.join_thread()

    def publish(self, topic, message):
        """
        Publishes a message on the supplied topic.

        :param str topic: topic to publish on
        :param bytes message: message content
        """
        LOGGER.debug("publish")

        self._work_queue.put(Publish(topic, message))

    def rpc_request(self, receiver, message, correlation_id, reply_to):
        """
        Interface for sending an RPC request to the producer connection.

        :param receiver: receiving RPC queue name
        :param message: contents to send
        :param correlation_id: identifies the request
        :param reply_to: response queue name
        """
        LOGGER.info("rpc_request receiver: {} message: {} "
                    "correlation_id: {} reply_to: {}"
                    .format(receiver, message, correlation_id, reply_to))

        self._work_queue.put(RPCRequest(receiver,
                                        message,
                                        correlation_id,
                                        reply_to))

    def rpc_response(self, receiver, message, correlation_id):
        """
        Interface for sending an RPC response to the producer connection.

        :param receiver: receiving RPC response queue name
        :param message: contents to send
        :param correlation_id: identifies which request the response belongs to
        """
        LOGGER.info("rpc_reply receiver: {} message: {} "
                    "correlation_id: {}".format(receiver, message,
                                                correlation_id))

        self._work_queue.put(RPCResponse(receiver,
                                         message,
                                         correlation_id))
