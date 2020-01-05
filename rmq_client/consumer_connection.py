import signal
import functools

from threading import Thread
from multiprocessing import Queue as IPCQueue

from .defs import Subscription, EXCHANGE_TYPE_FANOUT, ConsumedMessage
from .connection import RMQConnection


def create_consumer_connection(work_queue, consumed_messages):
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
    """
    consumer_connection = RMQConsumerConnection(work_queue, consumed_messages)
    consumer_connection.connect()


class RMQConsumerConnection(RMQConnection):
    """
    Class RMQConsumerConnection

    Specific connection implementation for RMQ Consumers. This class ensures:
    1. handling of channels according to what is needed for a RabbitMQ consumer.
    2. listening on a process shared work queue where work can be posted towards
       the RMQ consumer connection, for example to subscribe to a new topic.
    3. posting consumed messages to a process shared queue so that incoming
       messages can be read and handled by the controlling process.
    """

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

        :param pika.SelectConnection _connection: established connection
        """
        print("consumer connection open")
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """
        Callback for when a channel has been established on the connection.

        :param pika.channel.Channel channel: the opened channel
        """
        print("consumer connection channel open")
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)

        self.consumer_connection_started()

    def on_channel_closed(self, channel, reason):
        """
        Callback for when a channel has been closed.

        :param pika.channel.Channel channel: the channel that was closed
        :param Exception reason: exception explaining why the channel was closed
        """
        print("consumer connection channel {} closed for reason: {}".format(
            channel, reason))

    def consumer_connection_started(self):
        """
        Shall be called when the consumer connection has been established to the
        point where it is ready to receive work from its controlling process.
        In this case, when a channel has been established.
        """
        print("consumer connection started")
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
        print("consumer connection monitoring work queue")
        work = self._work_queue.get()  # Blocking, set block=false to not
        self.handle_work(work)
        self.monitor_work_queue()  # Recursive call

    def handle_work(self, work):
        """
        Handler for work posted on the work_queue, dispatches the work depending
        on the type of work.

        :param Subscription work: incoming work to be handled
        """
        print("consumer connection got work: {}".format(work))
        if isinstance(work, Subscription):
            self.handle_subscription(work)

    def handle_subscription(self, subscription: Subscription):
        """
        Handler for subscription work.

        :param Subscription subscription: information about a subscription
        """
        print("consumer connection handle_subscription()")
        cb = functools.partial(self.on_exchange_declared,
                               exchange_name=subscription.topic)
        self._channel.exchange_declare(exchange=subscription.topic,
                                       exchange_type=EXCHANGE_TYPE_FANOUT,
                                       callback=cb)

    def on_exchange_declared(self, _frame, exchange_name=None):
        """
        Callback for when an exchange has been declared.

        :param pika.frame.Method _frame: message frame
        :param str exchange_name: additional parameter from functools.partial,
                                  used to carry the exchange_name
        """
        print(
            "consumer connection on_exchange_declared(), exchange name: {}".format(
                exchange_name))
        print("exchange declared message frame: {}".format(_frame))
        cb = functools.partial(self.on_queue_declared,
                               exchange_name=exchange_name)
        self._channel.queue_declare(queue="", exclusive=True, callback=cb)

    def on_queue_declared(self, frame, exchange_name=None):
        """
        Callback for when a queue has been declared.

        :param pika.frame.Method frame: message frame
        :param str exchange_name: additional parameter from functools.partial,
                                  used to carry the exchange_name
        """
        print("consumer connection on_queue_declared(), queue name: {}".format(
            frame.method.queue))
        print("queue declared message frame: {}".format(frame))
        cb = functools.partial(self.on_queue_bound,
                               exchange_name=exchange_name,
                               queue_name=frame.method.queue)
        self._channel.queue_bind(
            frame.method.queue, exchange_name, callback=cb
        )

    def on_queue_bound(self, _frame, exchange_name=None, queue_name=None):
        """
        Callback for when a queue has been bound to an exchange.

        :param pika.frame.Method _frame: message frame
        :param str exchange_name: additional parameter from functools.partial,
                                  used to carry the exchange_name
        :param str queue_name: additional parameter from functools.partial, used
                               to carry the exchange_name
        """
        print("consumer connection on_queue_bound()")
        print("queue bound message frame: {}".format(_frame))
        print("bound queue {} to exchange {}".format(queue_name, exchange_name))
        self.consume(queue_name)

    def consume(self, queue_name):
        """
        Starts consuming on the parameter queue.

        :param str queue_name: name of the queue to consume from
        """
        print("consumer connection consume()")
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)
        self._channel.basic_consume(queue_name, self.on_message, exclusive=True)

    def on_message(self, _channel, basic_deliver, properties, body):
        """
        Callback for when a message is received on a consumed queue.

        :param _channel: channel that the message was received on
        :param pika.spec.Basic.Deliver basic_deliver: method
        :param pika.spec.BasicProperties properties: properties of the message
        :param bytes body: message body
        """
        print("consumer connection on_message()")
        print("message basic.deliver method: {}".format(basic_deliver))
        print("message properties: {}".format(properties))
        print("message body: {}".format(body))
        self._consumed_messages.put(ConsumedMessage(basic_deliver.exchange, body))

        self._channel.basic_ack(basic_deliver.delivery_tag)

    def on_consumer_cancelled(self, _frame):
        """
        Callback for when a consumer is cancelled by the RMQ server.

        :param pika.frame.Method _frame: message frame
        """
        print("consumer connection on_consumer_cancelled()")

    def interrupt(self, _signum, _frame):
        """
        Signal handler for signal.SIGINT.

        :param int _signum: signal.SIGINT
        :param ??? _frame: current stack frame
        """
        print("consumer connection interrupt")
        self._closing = True
        self.disconnect()

    def terminate(self, _signum, _frame):
        """
        Signal handler for signal.SIGTERM.

        :param int _signum: signal.SIGTERM
        :param ??? _frame: current stack frame
        """
        print("consumer connection terminate")
        self._closing = True
        self.disconnect()
