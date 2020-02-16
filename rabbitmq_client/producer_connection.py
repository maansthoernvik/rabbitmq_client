import signal

from threading import Thread
from multiprocessing import Queue as IPCQueue

from .producer_channel import RMQProducerChannel
from .log import LogClient
from .connection import RMQConnection


def create_producer_connection(work_queue, log_queue):
    """
    Interface function to instantiate and connect a producer connection. This
    function is intended as a target for a new process to avoid having to
    instantiate the RMQProducerConnection outside of the new process' memory
    context.

    :param work_queue: process shared queue used to issue work for the
                       producer connection
    :param log_queue: queue to post log writes to
    """
    producer_connection = RMQProducerConnection(work_queue, log_queue)
    producer_connection.connect()


class RMQProducerConnection(RMQConnection):
    """
    This class handles a connection to a RabbitMQ server intended for a producer
    entity. Messages to be published are posted to a process shared queue which
    is read continuously by a connection process-local thread assigned to
    monitoring the queue.
    """
    # general
    _log_client: LogClient

    # Connection
    _channel = None

    # IPC
    _work_queue: IPCQueue

    def __init__(self, work_queue, log_queue):
        """
        Initializes the RMQProducerConnection's work queue and binds signal
        handlers. The work queue can be used to issue commands.

        :param IPCQueue work_queue: process shared queue used to issue work for
                                    the consumer connection
        :param log_queue: process shared queue used to post messages to the
                          logging process
        """
        self._log_client = LogClient(log_queue, RMQProducerConnection.__name__)
        self._log_client.debug("__init__")

        self._work_queue = work_queue

        self._channel = RMQProducerChannel(log_queue)

        signal.signal(signal.SIGINT, self.interrupt)
        signal.signal(signal.SIGTERM, self.terminate)

        super().__init__()

    def on_connection_open(self, connection):
        """
        Callback when a connection has been established to the RMQ server.

        :param pika.SelectConnection connection: established connection
        """
        self._log_client.info("on_connection_open connection: {}"
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
            self._log_client.info(log_string)
            self.finalize_disconnect()
        else:
            # TODO: reconnect handling goes here
            self._log_client.critical(log_string)
            self.finalize_disconnect()

    def on_channel_open(self):
        """
        Callback for notifying a channel has been completely established.
        """
        self._log_client.debug("on_channel_open")

        self.producer_connection_started()

    def producer_connection_started(self):
        """
        Shall be called when the producer connection has reached a state where
        it is ready to receive and execute work, for instance to publish
        messages.
        """
        self._log_client.info("producer_connection_started")

        thread = Thread(target=self.monitor_work_queue, daemon=True)
        thread.start()

    def monitor_work_queue(self):
        """
        NOTE!

        This function should live in its own thread so that the
        RMQProducerConnection is able to respond to incoming work as quickly as
        possible.

        Monitors the producer connection's work queue and executes from it as
        soon as work is available.
        """
        while True:
            self._log_client.debug("monitor_work_queue waiting for work")

            # Blocking, set block=false to not block
            work = self._work_queue.get()
            self._channel.handle_work(work)

    def interrupt(self, _signum, _frame):
        """
        Signal handler for signal.SIGINT.

        :param int _signum: signal.SIGINT
        :param ??? _frame: current stack frame
        """
        self._log_client.debug("interrupt")

        self.disconnect()

    def terminate(self, _signum, _frame):
        """
        Signal handler for signal.SIGTERM.

        :param int _signum: signal.SIGTERM
        :param ??? _frame: current stack frame
        """
        self._log_client.debug("terminate")

        self.disconnect()