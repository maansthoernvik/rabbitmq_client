import signal

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
        print("Connection open")

    def interrupt(self, _signum, _frame):
        """
        Signal handler for signal.SIGINT

        :param _signum: signal.SIGINT
        :param _frame: current stack frame
        :return: None
        """
        self._closing = True
        self.disconnect()

    def terminate(self, _signum, _frame):
        """
        Signal handler for signal.SIGTERM

        :param _signum: signal.SIGTERM
        :param _frame: current stack frame
        :return: None
        """
        self._closing = True
        self.disconnect()
