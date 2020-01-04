import signal
import functools

from threading import Thread
from multiprocessing import Queue as IPCQueue

from .defs import Subscription, EXCHANGE_TYPE_FANOUT, Message
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
        """

        :param work:
        """
        print("consumer connection got work: {}".format(work))
        if isinstance(work, Subscription):
            self.handle_subscription(work)

    def handle_subscription(self, subscription):
        """

        :param topic:
        """
        print("consumer connection handle_subscription()")
        cb = functools.partial(self.on_exchange_declared,
                               exchange_name=subscription.topic)
        self._channel.exchange_declare(exchange=subscription.topic,
                                       exchange_type=EXCHANGE_TYPE_FANOUT,
                                       callback=cb)

    def on_exchange_declared(self, _frame, exchange_name=None):
        """

        :param exchange_name:
        :param _frame:
        """
        print("consumer connection on_exchange_declared(), exchange name: {}".format(exchange_name))
        print("exchange declared message frame: {}".format(_frame))
        cb = functools.partial(self.on_queue_declared,
                               exchange_name=exchange_name)
        self._channel.queue_declare(queue="", callback=cb)

    def on_queue_declared(self, frame, exchange_name=None):
        """

        :param frame:
        :param exchange_name:
        """
        print("consumer connection on_queue_declared(), queue name: {}".format(frame.method.queue))
        print("queue declared message frame: {}".format(frame))
        cb = functools.partial(self.on_queue_bound,
                               exchange_name=exchange_name,
                               queue_name=frame.method.queue)
        self._channel.queue_bind(
            frame.method.queue, exchange_name, callback=cb
        )

    def on_queue_bound(self, _frame, exchange_name=None, queue_name=None):
        """

        :param _frame:
        """
        print("consumer connection on_queue_bound()")
        print("queue bound message frame: {}".format(_frame))
        print("bound queue {} to exchange {}".format(queue_name, exchange_name))
        self.consume(queue_name)

    def consume(self, queue_name):
        """

        :param queue_name:
        """
        print("consumer connection consume()")
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)
        self._channel.basic_consume(queue_name, self.on_message)

    def on_message(self, _channel, basic_deliver, properties, body):
        """

        :param _channel:
        :param basic_deliver:
        :param properties:
        :param body:
        """
        print("consumer connection on_message()")
        print("message basic.deliver method: {}".format(basic_deliver))
        print("message properties: {}". format(properties))
        print("message body: {}".format(body))
        self._consumed_messages.put(Message("test", body))

        self._channel.basic_ack(basic_deliver.delivery_tag)

    def on_consumer_cancelled(self, _frame):
        """

        :param _frame:
        """
        print("consumer connection on_consumer_cancelled()")

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
