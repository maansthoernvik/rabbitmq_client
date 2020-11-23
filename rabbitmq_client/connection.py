import pika
import logging

from abc import ABCMeta, abstractmethod


LOGGER = logging.getLogger(__name__)


class RMQConnection(metaclass=ABCMeta):
    """
    Abstract class implementing the basics of a RabbitMQ server connection.
    This class does not meddle with channels as the handling of a channel may
    differ between consumers and producers. The RMQConnection class therefore
    only handles the connection (pika.SelectConnection) object itself.

    Subclasses inheriting from RMQConnection have to override the
    on_connection_open(connection: SelectConnection) function to take over once
    the connection has been established. At this point, it is possible to start
    creating channels. Subclasses also need to override
    on_connection_closed(connection: SelectConnection, reason) since consumers
    and producers differ quite a bit in how they handle their connection and
    channels in event of an error.
    """

    _connection_parameters: pika.ConnectionParameters
    _connection: pika.SelectConnection

    _closing: bool

    def __init__(self,
                 connection_parameters):
        """
        Initializes the RMQ connection with connection parameters and the
        general state of the RMQConnection adapter.
        :param connection_parameters: pika.ConnectionParameters or None
        """
        LOGGER.debug("__init__ of connection")

        if connection_parameters:
            self._connection_parameters = connection_parameters
        else:
            # Default credentials: guest/guest
            # Default host: localhost
            # Default vhost: /
            # Default port: 5672
            self._connection_parameters = pika.ConnectionParameters()

        self._connection = pika.SelectConnection(
            parameters=self._connection_parameters,
            on_open_callback=self.on_connection_open,
            on_close_callback=self.on_connection_closed
        )

        self._closing = False

    def connect(self):
        """
        Initiates the connection to the RabbitMQ server by starting the
        connection's IO-loop. Starting the IO-loop will connect to the RabbitMQ
        server as configured in __init__.
        """
        LOGGER.debug("connect in connection")

        self._connection.ioloop.start()

    @abstractmethod
    def on_connection_open(self, _connection):
        """
        Callback upon opened connection. Subclasses shall override this
        function in order to be notified when a connection has been established
        in order for them to know when they are able to create channels.

        :param pika.SelectConnection _connection: connection that was opened
        """
        pass

    @abstractmethod
    def on_connection_closed(self, _connection, reason):
        """
        Callback upon closed connection. Subclasses shall override this
        function in order to be notified when a connection has been closed.

        :param pika.SelectConnection _connection: connection that was closed
        :param Exception reason: reason for closing
        """
        pass

    def disconnect(self):
        """
        Disconnects from the RabbitMQ server by calling close() on the
        connection object. This operation should result in on_connection_closed
        being invoked once the connection has been closed, allowing for further
        handling of either gracefully shutting down or re-connecting.
        """
        LOGGER.debug("disconnect in connection")

        if not self._closing:
            self._closing = True
            self._connection.close()
            return

    def finalize_disconnect(self):
        """
        Shall be called once the connection has been closed to stop the
        IO-Loop.
        """
        LOGGER.debug("finalizing disconnect")

        self._connection.ioloop.stop()
