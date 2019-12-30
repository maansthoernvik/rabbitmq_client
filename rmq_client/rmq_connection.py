import signal
from threading import Thread

import pika
import multiprocessing

from multiprocessing import Event


connection_count = 0


def get_connection_count():
    return connection_count


class RMQConnection:

    _connection: pika.SelectConnection
    _connection_parameters: pika.ConnectionParameters

    start_flag: Event
    conn_id = 0

    _thread: Thread

    def __init__(self):
        """
        Constructor for the RMQConnection.
        """
        self._connection_parameters = pika.ConnectionParameters()
        self.start_flag = Event()

        global connection_count
        connection_count += 1
        self.conn_id = connection_count

    def start(self):
        # Good for being able to interrupt if gets stuck and still shut it down
        # gracefully.
        signal.signal(signal.SIGTERM, self.terminate)

        self._thread = Thread(target=self.run)
        self._thread.start()

        print("Start flag was set: {}".format(self.start_flag.is_set()))
        self.start_flag.wait()

        print("After starting, my _connection param is: {}".format(self._connection))

    def run(self):
        self._connection = pika.SelectConnection(
            self._connection_parameters,
            on_open_callback=self.on_connection_open,
            on_close_callback=self.on_connection_closed
        )
        print("Inside the other process, _connection: {}".format(self._connection))
        print("Inside the other process, connection params: {}".format(self._connection_parameters))
        self._connection.ioloop.start()

    def on_connection_open(self, _connection):
        print("on connection open")
        self.start_flag.set()

    def on_connection_closed(self, _unused_connection, reason):
        print(reason)
        self.start_flag.clear()

    def open_channel(self):
        print("Opening a channel")
        print("Number of connections: {}".format(get_connection_count()))
        print("RMQ connection information:")
        print("Start flag is set: {}".format(self.start_flag.is_set()))
        print("Process is alive: {}".format(self._thread.is_alive()))
        print("My connection id = {}".format(self.conn_id))
        print("_connection: {}".format(self._connection))

        channel = self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, _new_channel):
        print("on channel open")

    def terminate(self, _signum, _frame):
        print("SIGTERM received")
        self.stop()

    def close_connection(self):
        self._connection.close()

    def stop(self):
        self._connection.ioloop.stop()
