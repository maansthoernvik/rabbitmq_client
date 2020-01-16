import logging

from multiprocessing import Queue as IPCQueue

from .rpc import RMQRPCHandler
from .log import LogHandler, LogItem
from .consumer import RMQConsumer
from .producer import RMQProducer


class RMQClient:
    """
    Class RMQClient

    Handles connection to RabbitMQ and provides and interface for applications
    wanting to either publish
    """
    _log_handler: LogHandler

    _rpc_handler: RMQRPCHandler

    _consumer: RMQConsumer
    _producer: RMQProducer

    def __init__(self, log_level=logging.WARNING):
        """
        Initializes the RMQClient by initializing its member variables.
        """
        self._log_handler = LogHandler(IPCQueue(),
                                       log_level=log_level)
        self._log_handler.handle_log_item(
            LogItem("__init__", RMQClient.__name__, level=logging.DEBUG)
        )

        self._consumer = RMQConsumer(self._log_handler.get_log_queue())
        self._producer = RMQProducer(self._log_handler.get_log_queue())

        self._rpc_handler = RMQRPCHandler(self._consumer,
                                          self._producer,
                                          self._log_handler.get_log_queue())

    def start(self):
        """
        Starts the RMQClient by starting its subsequent consumer, producer, and
        RPC handler instances.
        """
        self._log_handler.handle_log_item(
            LogItem("start", RMQClient.__name__)
        )
        self._log_handler.start()

        self._consumer.start()
        self._producer.start()

        self._rpc_handler.start()

    def stop(self):
        """
        Stops the RMQ client and its child applications/processes.
        :return:
        """
        self._log_handler.handle_log_item(
            LogItem("stop", RMQClient.__name__)
        )
        self._consumer.stop()
        self._producer.stop()
        self._rpc_handler.stop()

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
        self._log_handler.handle_log_item(
            LogItem("Subscribe to topic: {}".format(topic),
                    RMQClient.__name__, level=logging.DEBUG)
        )
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
        self._log_handler.handle_log_item(
            LogItem("Publish to topic: {} message: {}".format(topic, message),
                    RMQClient.__name__, level=logging.DEBUG)
        )
        # publishes a message to the provided topic
        self._producer.publish(topic, message)

    def enable_rpc_server(self, rpc_server_name, rpc_request_callback):
        self._rpc_handler.enable_rpc_server(rpc_server_name,
                                            rpc_request_callback)

    def enable_rpc_client(self):
        self._rpc_handler.enable_rpc_client()

    def rpc_call(self, receiver, message):
        """

        :param receiver:
        :param message:
        :return:
        """
        self._log_handler.handle_log_item(
            LogItem("RPC call to receiver: {} message: {}"
                    .format(receiver, message), RMQClient.__name__)
        )
        return self._rpc_handler.rpc_call(receiver, message)

    def rpc_cast(self, receiver, message, callback):
        raise NotImplementedError
