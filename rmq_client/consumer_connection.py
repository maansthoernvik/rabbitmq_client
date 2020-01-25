import signal

from threading import Thread
from multiprocessing import Queue as IPCQueue

from .consumer_channel import RMQConsumerChannel
from .log import LogClient
from .connection import RMQConnection


def create_consumer_connection(work_queue, consumed_messages, log_queue):
    """
    Interface function to instantiate and connect a consumer connection. This
    function is intended as a target for a new process to avoid having to
    instantiate the RMQConsumerConnection outside of the new process' memory
    context.

    :param IPCQueue work_queue: process shared queue used to issue work for the
                                consumer connection
    :param IPCQueue consumed_messages: process shared queue used to forward
                                       messages received for a subscribed topic
                                       to the controlling process
    :param log_queue: queue to post log writes to
    """
    consumer_connection = RMQConsumerConnection(work_queue, consumed_messages,
                                                log_queue)
    consumer_connection.connect()


class RMQConsumerConnection(RMQConnection):
    """
    Specific connection implementation for RMQ Consumers. This class ensures:
    1. handling of channels according to what is needed for a RabbitMQ consumer.
    2. listening on a process shared work queue where work can be posted towards
       the RMQ consumer connection, for example to subscribe to a new topic.
    3. posting consumed messages to a process shared queue so that incoming
       messages can be read and handled by the controlling process.
    """
    # general
    _log_client: LogClient

    _channel: RMQConsumerChannel

    _work_queue: IPCQueue

    def __init__(self, work_queue, consumed_messages, log_queue):
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
        :param log_queue: process shared queue used to post messages to the
                          logging process
        """
        self._log_client = LogClient(log_queue, RMQConsumerConnection.__name__)
        self._log_client.debug("__init__")

        self._work_queue = work_queue

        self._channel = RMQConsumerChannel(consumed_messages, log_queue)

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

        self.consumer_connection_started()

    def consumer_connection_started(self):
        """
        Shall be called when the consumer connection has been established to the
        point where it is ready to receive work from its controlling process.
        In this case, when a channel has been established.
        """
        self._log_client.info("consumer_connection_started")

        thread = Thread(target=self.monitor_work_queue, daemon=True)
        thread.start()

    def monitor_work_queue(self):
        """
        NOTE!

        This function should live in its own thread so that the
        RMQConsumerConnection is able to respond to incoming work as quickly as
        possible.

        Monitors the consumer connection's work queue and executes from it as
        soon as work is available.
        """
        while True:
            self._log_client.debug("monitor_work_queue waiting for work")

            # Blocking, set block=false to not block
            consume = self._work_queue.get()
            self._channel.handle_consume(consume)

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
