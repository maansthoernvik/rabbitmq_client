import signal

from multiprocessing import Queue as IPCQueue

from .connection import RMQConnection


def create_consumer_connection(work_queue, consumed_messages):
    consumer_connection = RMQConsumerConnection(work_queue, consumed_messages)
    consumer_connection.connect()


class RMQConsumerConnection(RMQConnection):

    _work_queue: IPCQueue
    _consumed_messages: IPCQueue

    def __init__(self, work_queue, consumed_messages):
        self._work_queue = work_queue
        self._consumed_messages = consumed_messages

        signal.signal(signal.SIGINT, self.interrupt)
        signal.signal(signal.SIGTERM, self.terminate)

        super().__init__()

    def on_connection_open(self, _connection):
        print("Connection open")

    def interrupt(self, _signum, _frame):
        """
        Signal handler for signal.SIGINT

        :param _signum: signal.SIGINT
        :param _frame: ?
        :return: None
        """
        self._closing = True
        self.disconnect()

    def terminate(self, _signum, _frame):
        """
        Signal handler for signal.SIGTERM

        :param _signum: signal.SIGTERM
        :param _frame: ?
        :return: None
        """
        self._closing = True
        self.disconnect()
