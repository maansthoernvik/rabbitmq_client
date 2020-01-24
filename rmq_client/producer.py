from multiprocessing import Queue as IPCQueue, Process

from .log import LogClient
from .producer_connection import create_producer_connection
from .producer_defs import *


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
    Acts more as an interface towards the producer connection for the RPCHandler
    than actually doing something useful. Only instantiates work items and puts
    them on the work queue.
    """
    # general
    _log_client: LogClient

    # IPC
    _connection_process: Process

    _work_queue: IPCQueue

    def __init__(self, log_queue):
        """
        :param log_queue: queue to post logs to
        """
        self._log_client = LogClient(log_queue, RMQProducer.__name__)
        self._log_client.debug("__init__")

        self._work_queue = IPCQueue()

    def start(self):
        """
        Starts the producer's connection process in order to be able to publish
        messages. The connection process is maintained in another process, the
        work queue passed along to the new process is process shared to allow
        for the controlling process to issue commands to the connection process,
        for example to publish messages.
        """
        self._log_client.info("start")

        self._connection_process = Process(
            target=create_producer_connection,
            args=(self._work_queue, self._log_client.get_log_queue())
        )
        self._connection_process.start()

    def stop(self):
        """
        Stops the RMQConsumer, tearing down the RMQProducerConnection process.
        """
        self._log_client.info("stop")

        self._connection_process.terminate()
        self._connection_process.join(timeout=2)

    def publish(self, topic, message):
        """
        Publishes a message on the supplied topic.

        :param str topic: topic to publish on
        :param bytes message: message content
        """
        self._log_client.debug("publish")

        self._work_queue.put(Publish(topic, message))

    def rpc_request(self, receiver, message, correlation_id, reply_to):
        """
        Interface for sending an RPC request to the producer connection.

        :param receiver: receiving RPC queue name
        :param message: contents to send
        :param correlation_id: identifies the request
        :param reply_to: response queue name
        """
        self._log_client.info("rpc_request receiver: {} message: {} "
                              "correlation_id: {} reply_to: {}"
                              .format(receiver, message, correlation_id,
                                      reply_to))

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
        self._log_client.info("rpc_reply receiver: {} message: {} "
                              "correlation_id: {}".format(receiver, message,
                                                          correlation_id))

        self._work_queue.put(RPCResponse(receiver,
                                         message,
                                         correlation_id))

