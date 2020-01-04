import signal
import functools

from threading import Thread
from multiprocessing import Queue as IPCQueue

from .connection import RMQConnection


def create_consumer_connection(work_queue, consumed_messages):
    """
    Interface function to instantiate and connect a consumer connection. This
    function is intended as a target for a new process to avoid having to
    instantiate the RMQConsumerConnection outside of the new process' memory
    context.

    :param work_queue: process shared queue used to issue work for the
                       consumer connection
    :param consumed_messages: process shared queue used to forward messages
                              received for a subscribed topic to the
                              controlling process
    """
    consumer_connection = RMQConsumerConnection(work_queue, consumed_messages)
    consumer_connection.connect()


class RMQConsumerConnection(RMQConnection):

    _channel = None

    _work_queue: IPCQueue
    _consumed_messages: IPCQueue

    def __init__(self, work_queue, consumed_messages):
        """
        Initializes the RMQConsumerConnection with two queues and binds signal
        handlers. The two queues are used to communicate between the connection
        and controlling process. The work queue can be used to issue commands,
        and the consumed messages queue is used to forward received messages to
        the controlling process.

        :param work_queue: process shared queue used to issue work for the
                           consumer connection
        :param consumed_messages: process shared queue used to forward messages
                                  received for a subscribed topic to the
                                  controlling process
        """
        print("consumer connection __init__")
        self._work_queue = work_queue
        self._consumed_messages = consumed_messages

        signal.signal(signal.SIGINT, self.interrupt)
        signal.signal(signal.SIGTERM, self.terminate)

        super().__init__()

    def on_connection_open(self, _connection):
        """
        Callback when a connection has been established to the RMQ server.

        :param _connection: established connection
        """
        print("consumer connection open")
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        print("consumer connection channel open")
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)

        self.consumer_connection_started()

    def on_channel_closed(self, channel, reason):
        print("consumer connection channel {} closed for reason: {}".format(channel, reason))

    def consumer_connection_started(self):
        print("consumer connection started")
        thread = Thread(target=self.monitor_work_queue, daemon=True)
        thread.start()

    def monitor_work_queue(self):
        print("consumer connection monitoring work queue")
        work = self._work_queue.get()
        self.handle_work(work)
        self.monitor_work_queue()

    def handle_work(self, work):
        print("consumer connection got work: {}".format(work))

    def interrupt(self, _signum, _frame):
        """
        Signal handler for signal.SIGINT

        :param _signum: signal.SIGINT
        :param _frame: current stack frame
        :return: None
        """
        print("consumer connection interrupt")
        self._closing = True
        self.disconnect()

    def terminate(self, _signum, _frame):
        """
        Signal handler for signal.SIGTERM

        :param _signum: signal.SIGTERM
        :param _frame: current stack frame
        :return: None
        """
        print("consumer connection terminate")
        self._closing = True
        self.disconnect()
