import pika

from abc import ABCMeta, abstractmethod


class RMQConnection(metaclass=ABCMeta):

    _connection_parameters: pika.ConnectionParameters
    _connection: pika.SelectConnection

    _closing: bool
    _connected: bool

    def __init__(self):
        """
        Initializes the RMQ connection with connection parameters.
        """
        self._connection_parameters = pika.ConnectionParameters()

        self._connection = pika.SelectConnection(
            parameters=self._connection_parameters,
            on_open_callback=self.on_connection_open,
            on_close_callback=self.on_connection_closed
        )

        self._closing = False
        self._connected = False

    def connect(self):
        """
        Initiates the connection to the RabbitMQ server by starting the
        connection's IO-loop.

        :return: None
        """
        self._connection.ioloop.start()

    @abstractmethod
    def on_connection_open(self, _connection):
        """
        Callback upon opened connection.

        :param _connection: connection that was opened.
        :return: None
        """
        pass

    def on_connection_closed(self, _connection, reason):
        """
        Callback upon closed connection.

        :param _connection: connection that was closed
        :param reason: reason for closing
        :return: None
        """
        print("connection closed: {}".format(reason))
        if self._closing:
            print("stopping ioloop")
            self._connection.ioloop.stop()
        else:
            print("connection closed unexpectedly")

    def disconnect(self):
        """
        Disconnects from the RabbitMQ server.
        """
        print("closing connection")
        self._connection.close()
