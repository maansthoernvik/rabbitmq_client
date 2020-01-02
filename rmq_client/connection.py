import time

import pika
import signal
import multiprocessing


class RMQConnection:

    # Process external variables
    _connection_process: multiprocessing.Process

    # Process internal variables, shall only be modified from within the
    # connection process!
    _closing: bool

    def __init__(self):
        """
        Initialized the RMQ connection with connection parameters. These
        parameters are only relevant prior to connecting and after that point
        cannot be changed to alter any aspect of the connection to the RabbitMQ
        server. Once started, the connection lives in a separate process meaning
        changes to the connection parameters are pointless as memory is not
        shared between python processes.
        """
        self._connection_parameters = pika.ConnectionParameters()

    def connect(self):
        """
        Initiates the connection to the RabbitMQ server in a separate process.
        A reference to the started process can be found in
        self._connection_process.

        :return: None
        """
        self._connection_process = \
            multiprocessing.Process(target=self._initiate_connection_process,
                                    args=(self._connection_parameters,))
        self._connection_process.start()

    def _initiate_connection_process(self, connection_parameters):
        """
        Initiates the connection process by binding terminate and interrupt
        signals to disconnection handlers and starts the connection towards RMQ.

        :param connection_parameters: parameters used to connect to the RMQ
                                      server.
        :return: None
        """
        signal.signal(signal.SIGTERM, self.terminate)
        signal.signal(signal.SIGINT, self.interrupt)

        self._connection_parameters = connection_parameters
        self._connection = pika.SelectConnection(
            parameters=connection_parameters,
            on_open_callback=self.on_connection_open,
            on_close_callback=self.on_connection_closed
        )

        self._connection.ioloop.start()

    def on_connection_open(self, _connection):
        """
        Callback upon opened connection.

        :param _connection: connection that was opened.
        :return: None
        """
        print("connection opened")

    def on_connection_closed(self, _connection, reason):
        """
        Callback upon closed connection.

        :param _connection: connection that was closed
        :param reason: reason for closing
        :return: None
        """
        print("connection closed: {}".format(reason))
        if self._closing:
            self._connection.ioloop.stop()
        else:
            print("connection closed unexpectedly")

    def interrupt(self, _signum, _frame):
        """
        Signal handler for signal.SIGINT

        :param _signum: signal.SIGINT
        :param _frame: ?
        :return: None
        """
        self._closing = True
        self._disconnect()

    def terminate(self):
        """
        Interface for terminating the connection process and thereby closing
        the connection to RMQ.

        :return: None
        """
        self._connection_process.terminate()

    def terminate(self, _signum, _frame):
        """
        Signal handler for signal.SIGTERM

        :param _signum: signal.SIGTERM
        :param _frame: ?
        :return: None
        """
        self._closing = True
        self._disconnect()

    def _disconnect(self):
        """
        NOTE: NOT THREADSAFE! SHALL ONLY BE CALLED FROM THE CONNECTION PROCESS.
        OUTSIDE PROCESSES SHALL USE THE NO-ARG TERMINATE INTERFACE METHOD TO
        CANCEL THE CONNECTION.

        :return:
        """
        print("closing connection")
        self._connection.close()
