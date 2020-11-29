import signal
import logging

from threading import Thread

from .consumer_channel import RMQConsumerChannel
from .connection import RMQConnection
from rabbitmq_client import log


LOGGER = logging.getLogger(__name__)


def create_consumer_connection(connection_parameters,
                               work_queue,
                               consumed_messages,
                               log_queue,
                               log_level):
    """
    Interface function to instantiate and connect a consumer connection. This
    function is intended as a target for a new process to avoid having to
    instantiate the RMQConsumerConnection outside of the new process' memory
    context.

    :param connection_parameters: pika.ConnectionParameters or None
    :param IPCQueue work_queue: process shared queue used to issue work for the
                                consumer connection
    :param IPCQueue consumed_messages: process shared queue used to forward
                                       messages received for a subscribed topic
                                       to the controlling process
    :param log_queue: queue to post log writes to
    :param log_level: log level
    """
    # Configure logging
    log.set_process_log_handler(log_queue, log_level)

    consumer_connection = RMQConsumerConnection(connection_parameters,
                                                work_queue,
                                                consumed_messages)
    consumer_connection.connect()


class RMQConsumerConnection(RMQConnection):
    """
    Specific connection implementation for RMQ Consumers. This class ensures:
    1. handling of channels according to what is needed for a RabbitMQ
       consumer.
    2. listening on a process shared work queue where work can be posted
       towards the RMQ consumer connection, for example to subscribe to a new
       topic.
    3. posting consumed messages to a process shared queue so that incoming
       messages can be read and handled by the controlling process.
    """

    def __init__(self,
                 connection_parameters,
                 work_queue,
                 consumed_messages):
        """
        Initializes the RMQConsumerConnection with two queues and binds signal
        handlers. The two queues are used to communicate between the connection
        and controlling process. The work queue can be used to issue commands,
        and the consumed messages queue is used to forward received messages to
        the controlling process.

        :param connection_parameters: pika.ConnectionParameters or None
        :param work_queue: process shared queue used to issue work for the
                           consumer connection
        :param consumed_messages: process shared queue used to forward messages
                                  received for a subscribed topic to the
                                  controlling process
        """
        LOGGER.debug("__init__")

        self._work_queue = work_queue

        self._channel = RMQConsumerChannel(consumed_messages)
        self._work_thread = Thread(target=self.monitor_work_queue, daemon=True)

        signal.signal(signal.SIGINT, self.interrupt)
        signal.signal(signal.SIGTERM, self.terminate)

        super().__init__(connection_parameters)

    def on_connection_open(self, connection):
        """
        Callback when a connection has been established to the RMQ server.

        :param pika.SelectConnection connection: established connection
        """
        LOGGER.info("on_connection_open connection: {}".format(connection))

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

        self.consumer_connection_started()

    def consumer_connection_started(self):
        """
        Shall be called when the consumer connection has been established to
        the point where it is ready to receive work from its controlling
        process. In this case, when a channel has been established.
        """
        LOGGER.info("consumer_connection_started")

        self._work_thread.start()

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
            LOGGER.debug("monitor_work_queue waiting for work")

            # Blocking, set block=false to not block
            consume = self._work_queue.get()
            LOGGER.debug("got work to do")
            self._channel.handle_consume(consume)

    def interrupt(self, _signum, _frame):
        """
        Signal handler for signal.SIGINT.

        :param int _signum: signal.SIGINT
        :param ??? _frame: current stack frame
        """
        LOGGER.debug("interrupt")
        self.stop()

    def terminate(self, _signum, _frame):
        """
        Signal handler for signal.SIGTERM.

        :param int _signum: signal.SIGTERM
        :param ??? _frame: current stack frame
        """
        LOGGER.debug("terminate")
        self.stop()

    def stop(self):
        """General stop handler function."""
        self.disconnect()
