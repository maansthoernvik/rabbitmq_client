import signal
import functools

from threading import Thread
from multiprocessing import Queue as IPCQueue

from .defs import Publish, EXCHANGE_TYPE_FANOUT
from .connection import RMQConnection


def create_producer_connection(work_queue):
    """
    Interface function to instantiate and connect a producer connection. This
    function is intended as a target for a new process to avoid having to
    instantiate the RMQProducerConnection outside of the new process' memory
    context.

    :param work_queue: process shared queue used to issue work for the
                       producer connection
    """
    producer_connection = RMQProducerConnection(work_queue)
    producer_connection.connect()


class RMQProducerConnection(RMQConnection):
    """
    Class RMQProducerConnection

    This class handles a connection to a RabbitMQ server intended for a producer
    entity. Messages to be published are posted to a process shared queue which
    is read continuously by a connection process-local thread assigned to
    monitoring the queue.
    """

    _channel = None

    _work_queue: IPCQueue

    def __init__(self, work_queue):
        """
        Initializes the RMQProducerConnection's work queue and binds signal
        handlers. The work queue can be used to issue commands.

        :param IPCQueue work_queue: process shared queue used to issue work for
                                    the consumer connection
        """
        print("producer connection __init__")
        self._work_queue = work_queue

        signal.signal(signal.SIGINT, self.interrupt)
        signal.signal(signal.SIGTERM, self.terminate)

        super().__init__()

    def on_connection_open(self, _connection):
        """
        Callback when a connection has been established to the RMQ server.

        :param pika.SelectConnection _connection: established connection
        """
        print("producer connection open")
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """
        Callback for when a channel has been established on the connection.

        :param pika.channel.Channel channel: the opened channel
        """
        print("producer connection channel open")
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)

        self._channel.confirm_delivery(
            ack_nack_callback=self.on_delivery_confirmed,
            callback=self.on_confirm_mode_activated
        )

    def on_confirm_mode_activated(self, _frame):
        """
        Callback for when confirm mode has been activated.

        :param pika.frame.Method _frame: message frame
        """
        print("producer connection on_confirm_mode_activated()")
        print("Confirm.SelectOK: {}".format(_frame))
        self.producer_connection_started()

    def on_channel_closed(self, channel, reason):
        """
        Callback for when a channel has been closed.

        :param pika.channel.Channel channel: the channel that was closed
        :param Exception reason: exception explaining why the channel was closed
        """
        print("producer connection channel {} closed for reason: {}".format(channel, reason))

    def producer_connection_started(self):
        """
        Shall be called when the producer connection has reached a state where
        it is ready to receive and execute work, for instance to publish
        messages.
        """
        print("producer connection started")
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
        print("producer connection monitoring work queue")
        work = self._work_queue.get()
        self.handle_work(work)
        self.monitor_work_queue()

    def handle_work(self, work):
        """
        Handler for work posted on the work_queue, dispatches the work depending
        on the type of work.

        :param Publish work: incoming work to be handled
        """
        print("producer connection got work: {}".format(work))

        if isinstance(work, Publish):
            self.handle_publish(work)

    def handle_publish(self, publish: Publish):
        """
        Handler for publishing work.

        :param Publish publish: information about a publish
        """
        print("producer connection handle_publish()")
        cb = functools.partial(self.on_exchange_declared,
                               exchange_name=publish.topic,
                               message_content=publish.message_content)
        self._channel.exchange_declare(exchange=publish.topic,
                                       exchange_type=EXCHANGE_TYPE_FANOUT,
                                       callback=cb)

    def on_delivery_confirmed(self, frame):
        """
        Callback for when a publish is confirmed

        :param pika.frame.Method frame: message frame
        """
        print("producer connection on_delivery_confirmed()")
        print("delivery confirmed frame: {}".format(frame))

    def on_exchange_declared(self, _frame, exchange_name=None, message_content=None):
        """
        Callback for when an exchange has been declared.

        :param pika.frame.Method _frame: message frame
        :param str exchange_name: additional parameter from functools.partial,
                                  used to carry the exchange_name
        :param str message_content: additional parameter from functools.partial,
                                    used to carry the message_content
        """
        print("producer connection on_exchange_declared(), exchange name: {}".format(exchange_name))
        print("exchange declared message frame: {}".format(_frame))
        print("exchange declared message_content: {}".format(message_content))
        self.publish(exchange_name, message_content)

    def publish(self, exchange_name, message_content):
        """
        Publish the message to the named exchange.

        :param str exchange_name: name of exchange to publish to
        :param str message_content: content of message to publish
        """
        print("producer connection publish()")
        self._channel.basic_publish(exchange=exchange_name,
                                    routing_key="",
                                    body=message_content)

    def interrupt(self, _signum, _frame):
        """
        Signal handler for signal.SIGINT.

        :param int _signum: signal.SIGINT
        :param ??? _frame: current stack frame
        """
        print("producer connection interrupt")
        self._closing = True
        self.disconnect()

    def terminate(self, _signum, _frame):
        """
        Signal handler for signal.SIGTERM.

        :param int _signum: signal.SIGTERM
        :param ??? _frame: current stack frame
        """
        print("producer connection terminate")
        self._closing = True
        self.disconnect()
