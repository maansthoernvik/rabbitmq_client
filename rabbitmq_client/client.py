import logging

from multiprocessing import Queue as IPCQueue

from .rpc import RMQRPCHandler
from .log import LogHandler, LogClient
from .consumer import RMQConsumer
from .producer import RMQProducer


class RMQClient:
    """
    Handles connection to RabbitMQ and provides and interface for applications
    wanting to either publish, subscribe, or issue RPC requests.
    """
    _log_handler: LogHandler
    _log_client: LogClient

    _rpc_handler: RMQRPCHandler

    _consumer: RMQConsumer
    _producer: RMQProducer

    def __init__(self, log_level=logging.WARNING):
        """
        :param log_level: sets the log level of the RMQClient
        """
        self._log_handler = LogHandler(IPCQueue(),
                                       log_level=log_level)
        self._log_client = LogClient(self._log_handler.get_log_queue(),
                                     RMQClient.__name__)
        self._log_client.debug("__init__")

        self._consumer = RMQConsumer(self._log_handler.get_log_queue())
        self._producer = RMQProducer(self._log_handler.get_log_queue())

        self._rpc_handler = RMQRPCHandler(self._consumer,
                                          self._producer,
                                          self._log_handler.get_log_queue())

    def start(self):
        """
        Starts the RMQClient by starting its subsequent consumer and producer
        connections.
        """
        self._log_client.info("start")
        self._log_handler.start()

        self._consumer.start()
        self._producer.start()

    def stop(self):
        """
        Stops the RMQ client and its child applications/processes.
        """
        self._log_client.info("stop")
        self._consumer.stop()
        self._producer.stop()

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
        self._log_client.info("subscribe topic: {} callback: {}"
                              .format(topic, callback))
        # dynamically add a subscription to a topic
        self._consumer.subscribe(topic, callback)

    def is_subscribed(self, topic) -> bool:
        """
        Checks if a subscription has been activated, meaning RMQ has confirmed
        with a ConsumeOk that a consume is active.

        :param topic: topic to check
        :return: true if active
        """
        self._log_client.debug("is_subscribed topic: {}".format(topic))

        return self._consumer.is_subscribed(topic)

    def publish(self, topic, message):
        """
        Publishes a message on the given topic. The publising work is dispatched
        to another process, meaning publishing probably has not completed by the
        time this function returns.

        :param str topic: topic to publish on
        :param bytes message: message to publish
        """
        self._log_client.info("publish to topic: {} message: {}"
                              .format(topic, message))
        # publishes a message to the provided topic
        self._producer.publish(topic, message)

    def enable_rpc_server(self, rpc_queue_name, rpc_request_callback):
        """
        Enables an RPC server for the supplied RPC queue name. The client will
        subscribe to messages on the supplied queue and relay incoming requests
        to the supplied callback.

            rpc_request_callback(message: bytes) -> bytes

         !!! NOTE The importance of the supplied callback to RETURN bytes. !!!

        :param rpc_queue_name: string name of the RPC request queue
        :param rpc_request_callback: callback called upon incoming request
        """
        self._log_client.info("enable_rpc_server rpc_queue_name: {} "
                              "rpc_request_callback: {}"
                              .format(rpc_queue_name, rpc_request_callback))
        self._rpc_handler.enable_rpc_server(rpc_queue_name,
                                            rpc_request_callback)

    def is_rpc_server_ready(self) -> bool:
        """
        Checks if the RPC server is ready, meaning it is consuming on the RPC
        server queue.

        :return: True if ready
        """
        self._log_client.info("is_rpc_server_ready")

        return self._rpc_handler.is_rpc_server_ready()

    def enable_rpc_client(self):
        """
        Enables the client to act as an RPC client. This will make sure that
        the client can handle sending RPC requests.
        """
        self._log_client.info("enable_rpc_client")
        self._rpc_handler.enable_rpc_client()

    def is_rpc_client_ready(self) -> bool:
        """
        Check if the RPC client is ready, meaning it is consuming on the RPC
        client's reply queue.

        :return: True if ready
        """
        self._log_client.info("is_rpc_client_ready")

        return self._rpc_handler.is_rpc_client_ready()

    def rpc_call(self, receiver, message) -> bytes:
        """
        NOTE! Must enable_rpc_client before making calls to this function.

        Make a synchronous call to an RPC server.

        :param str receiver: name of the RPC server to send the request to
        :param bytes message: message to send to the RPC server
        
        :return bytes answer: response message from the RPC server
        """
        self._log_client.info("rpc_call receiver: {} message: {}"
                              .format(receiver, message))
        return self._rpc_handler.rpc_call(receiver, message)

    def rpc_cast(self, receiver, message, callback):
        """
        NOTE! Must enable_rpc_client before making calls to this function.

        Make an asynchronous call to an RPC server.

        :param receiver: name of the RPC server to send the request to
        :param message: message to send to the RPC server
        :param callback: callback for when response is gotten
        """
        raise NotImplementedError
