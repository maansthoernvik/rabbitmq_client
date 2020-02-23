import logging

from rabbitmq_client import log

from .log import LogManager
from .rpc import RMQRPCHandler
from .consumer import RMQConsumer
from .producer import RMQProducer


LOGGER = logging.getLogger(__name__)


class RMQClient:
    """
    Handles connection to RabbitMQ and provides and interface for applications
    wanting to either publish, subscribe, or issue RPC requests.
    """

    def __init__(self, log_level=None):
        """
        :param log_level: sets the log level of the RMQClient, None being no
                          logging at all.
        """
        log.initialize_log_manager(log_level=log_level)

        log_manager: LogManager = log.get_log_manager()
        # Client process log handler is set here!
        log.set_process_log_handler(log_manager.log_queue, log_level)

        self._consumer = RMQConsumer()
        self._producer = RMQProducer()

        self._rpc_handler = RMQRPCHandler(self._consumer,
                                          self._producer)

    def start(self):
        """
        Starts the RMQClient by starting its subsequent consumer and producer
        connections.
        """
        LOGGER.info("start")

        self._consumer.start()
        self._producer.start()

    def stop(self):
        """
        Stops the RMQ client and its child applications/processes.
        """
        LOGGER.info("stop")
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
        LOGGER.info(f"subscribe topic: {topic} callback: {callback}")

        # dynamically add a subscription to a topic
        self._consumer.subscribe(topic, callback)

    def is_subscribed(self, topic) -> bool:
        """
        Checks if a subscription has been activated, meaning RMQ has confirmed
        with a ConsumeOk that a consume is active.

        :param topic: topic to check
        :return: true if active
        """
        LOGGER.debug(f"is_subscribed topic: {topic}")

        return self._consumer.is_subscribed(topic)

    def publish(self, topic, message):
        """
        Publishes a message on the given topic. The publising work is dispatched
        to another process, meaning publishing probably has not completed by the
        time this function returns.

        :param str topic: topic to publish on
        :param bytes message: message to publish
        """
        LOGGER.info("publish to topic: {} message: {}".format(topic, message))
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
        LOGGER.info("enable_rpc_server rpc_queue_name: {} "
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
        LOGGER.info("is_rpc_server_ready")

        return self._rpc_handler.is_rpc_server_ready()

    def enable_rpc_client(self):
        """
        Enables the client to act as an RPC client. This will make sure that
        the client can handle sending RPC requests.
        """
        LOGGER.info("enable_rpc_client")
        self._rpc_handler.enable_rpc_client()

    def is_rpc_client_ready(self) -> bool:
        """
        Check if the RPC client is ready, meaning it is consuming on the RPC
        client's reply queue.

        :return: True if ready
        """
        LOGGER.info("is_rpc_client_ready")

        return self._rpc_handler.is_rpc_client_ready()

    def rpc_call(self, receiver, message) -> bytes:
        """
        NOTE! Must enable_rpc_client before making calls to this function.

        Make a synchronous call to an RPC server.

        :param str receiver: name of the RPC server to send the request to
        :param bytes message: message to send to the RPC server
        
        :return bytes answer: response message from the RPC server
        """
        LOGGER.info("rpc_call receiver: {} message: {}"
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
