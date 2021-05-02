import logging

from .producer_channel import RMQProducerChannel
from .connection import RMQConnection


LOGGER = logging.getLogger(__name__)


class RMQProducerConnection(RMQConnection):
    """
    This class handles a connection to a RabbitMQ server intended for a
    producer entity.

    Messages to be published are posted to a process shared queue which
    is read continuously by a connection process-local thread assigned to
    monitoring the queue.
    """

    def __init__(self,
                 connection_parameters=None):
        """
        Initializes the RMQProducerConnection's work queue and binds signal
        handlers. The work queue can be used to issue commands.

        :param connection_parameters: connection parameters to the RMQ server
        :type connection_parameters: pika.ConnectionParameters
        """
        LOGGER.debug("__init__")

        self._channel = RMQProducerChannel()

        super().__init__(connection_parameters=connection_parameters)

    def on_connection_open(self, connection):
        """
        Callback when a connection has been established to the RMQ server.

        :param pika.SelectConnection connection: established connection
        """
        LOGGER.info("on_connection_open connection: {}"
                    .format(connection))

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

    def publish(self, publish):
        """"""
        self._channel.handle_produce(publish)

    def rpc_request(self, rpc_request):
        """"""
        self._channel.handle_produce(rpc_request)

    def rpc_response(self, rpc_response):
        """"""
        self._channel.handle_produce(rpc_response)

    def command(self, command):
        """"""
        self._channel.handle_produce(command)
