import logging

from .consumer_channel import RMQConsumerChannel
from .connection import RMQConnection


LOGGER = logging.getLogger(__name__)


class RMQConsumerConnection(RMQConnection):
    """
    Specific connection implementation for RMQ Consumers. This class ensures:
    1. handling of channels according to what is needed for a RabbitMQ
       consumer.
    2. listening on a process shared work queue where work can be posted
       towards the RMQ consumer connection, for example to subscribe to a new
       topic.
    3. posting consumed messages to a process shared queue so that incoming
       messages can be read and handled by the controlling process.
    """

    def __init__(self,
                 on_message_callback,
                 connection_parameters=None):
        """
        :param on_message_callback: callback for when a message is consumed
        :param connection_parameters: connection parameters to the RMQ server
        :type connection_parameters: pika.ConnectionParameters
        """
        LOGGER.debug("__init__")

        self._channel = RMQConsumerChannel(on_message_callback)

        super().__init__(connection_parameters=connection_parameters)

    def on_connection_open(self, connection):
        """
        Callback when a connection has been established to the RMQ server.

        :param pika.SelectConnection connection: established connection
        """
        LOGGER.info("on_connection_open connection: {}".format(connection))

        self._channel.open_channel(connection, self.on_channel_open)

    def on_connection_closed(self, _connection, reason):
        """
        Callback for when a connection is closed.

        :param _connection: connection that was closed
        :param reason: reason for closing
        """
        log_string = "on_connection_closed connection: {} reason: {}" \
            .format(_connection, reason)

        if self._closing:
            LOGGER.info(log_string)
            self.finalize_disconnect()
        else:
            # TODO: reconnect handling goes here
            LOGGER.critical(log_string)
            self.finalize_disconnect()

    def on_channel_open(self):
        """
        Callback for notifying a channel has been completely established.
        """
        LOGGER.debug("on_channel_open")

    def subscribe(self, subscription):
        """"""
        self._channel.handle_consume(subscription)

    def rpc_server(self, rpc_server):
        """"""
        self._channel.handle_consume(rpc_server)

    def rpc_client(self, rpc_client):
        """"""
        self._channel.handle_consume(rpc_client)

    def command_queue(self, command_queue):
        """"""
        self._channel.handle_consume(command_queue)
