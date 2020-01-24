import functools
from multiprocessing import Queue as IPCQueue

from .consumer_defs import Subscription, RPCServer, RPCClient, ConsumedMessage, \
    ConsumeOk
from .common_defs import AUTO_GEN_QUEUE_NAME, EXCHANGE_TYPE_FANOUT
from .log import LogClient

from pika.spec import FRAME_METHOD


class RMQConsumerChannel:
    """
    Defines handling of a consumer channel. Declarations of queues and
    exchanges, consuming and error handling.
    """
    # log
    _log_client: LogClient

    # pika channel
    _channel = None

    # message queue for consumed messages
    _consumed_messages: IPCQueue

    # channel state
    _open = False

    def __init__(self, consumed_messages, log_queue):
        """
        :param consumed_messages: IPC queue used to relay consumed messages
                                  between processes
        :param log_queue: IPC queue used to port logging messages back to main
                          process for writing to file
        """
        self._log_client = LogClient(log_queue, RMQConsumerChannel.__name__)
        self._log_client.debug("__init__")

        self._consumed_messages = consumed_messages

    def open_channel(self, connection, notify_callback):
        """
        Opens a channel for the parameter connection.

        :param connection: current connection object
        :param notify_callback: callback to notify once channel is ready for
                                work
        """
        self._log_client.debug("open_channel")

        cb = functools.partial(self.on_channel_open,
                               notify_callback=notify_callback)

        if not self._open:
            connection.channel(on_open_callback=cb)

    def on_channel_open(self, channel, notify_callback=None):
        """
        Callback for channel open.

        :param channel: opened channel
        :param notify_callback: callback to notify once channel is ready for
                                work
        """
        self._log_client.info("on_channel_open channel: {}".format(channel))

        self._open = True

        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)

        notify_callback()

    def on_channel_closed(self, _channel, reason):
        """
        Callback for channel closed.

        :param _channel: closed channel
        :param reason: reason channel was closed (exception)
        """
        self._log_client.info("on_channel_closed channel: {} reason: {}"
                              .format(_channel, reason))

        self._open = False

    def handle_consume(self, consume):
        """
        Initiates a new consumer for the channel.

        :param consume: consumer information needed to establish the new
                        consumer
        """
        self._log_client.debug("handle_consume consume: {}".format(consume))

        if isinstance(consume, Subscription):
            cb = functools.partial(self.on_exchange_declared,
                                   consume=consume)
            self._channel.exchange_declare(exchange=consume.topic,
                                           exchange_type=EXCHANGE_TYPE_FANOUT,
                                           callback=cb)

        elif isinstance(consume, RPCServer):
            cb = functools.partial(self.on_queue_declared,
                                   consume=consume)
            self._channel.queue_declare(queue=consume.queue_name,
                                        callback=cb)

        elif isinstance(consume, RPCClient):
            cb = functools.partial(self.on_queue_declared,
                                   consume=consume)
            self._channel.queue_declare(queue=consume.queue_name,
                                        exclusive=True,
                                        callback=cb)

    def on_exchange_declared(self, _frame, consume):
        """
        Callback for when an exchange has been declared.

        :param pika.frame.Method _frame: message frame
        :param consume: consumer information needed to establish the new
                        consumer
        """
        self._log_client.debug("on_exchange_declared frame: {} consume: {}"
                               .format(_frame, consume))

        cb = functools.partial(self.on_queue_declared,
                               consume=consume)
        self._channel.queue_declare(queue=AUTO_GEN_QUEUE_NAME,
                                    exclusive=True,
                                    callback=cb)

    def on_queue_declared(self, frame, consume):
        """
        Callback for when a queue has been declared.

        :param pika.frame.Method frame: message frame
        :param consume: consumer information needed to establish the new
                        consumer
        """
        self._log_client.debug("on_queue_declared frame: {} consume: {}"
                               .format(frame, consume))

        if isinstance(consume, Subscription):
            consume.set_queue_name(frame.method.queue)
            cb = functools.partial(self.on_queue_bound,
                                   consume=consume)
            self._channel.queue_bind(
                consume.queue_name, consume.topic, callback=cb
            )

        elif isinstance(consume, RPCServer) or \
                isinstance(consume, RPCClient):
            # No exchange = no need to bind the queue
            self.consume(consume)

    def on_queue_bound(self, _frame, consume):
        """
        Callback for when a queue has been bound to an exchange.

        :param pika.frame.Method _frame: message frame
        :param consume: consumer information needed to establish the new
                        consumer
        """
        self._log_client.debug("on_queue_bound frame: {} consume: {}"
                               .format(_frame, consume))

        self.consume(consume)

    def consume(self, consume):
        """
        Starts consuming on the parameter queue.

        :param consume: consume action
        """
        self._log_client.info("consume queue_name: {}"
                              .format(consume.queue_name))

        cb = functools.partial(self.on_consume_ok,
                               consume=consume)
        # All consumes so far are exclusive.
        self._channel.basic_consume(consume.queue_name,
                                    self.on_message,
                                    exclusive=True,
                                    callback=cb)

    def on_message(self, _channel, basic_deliver, properties, body):
        """
        Callback for when a message is received on a consumed queue.

        :param _channel: channel that the message was received on
        :param pika.spec.Basic.Deliver basic_deliver: method
        :param pika.spec.BasicProperties properties: properties of the message
        :param bytes body: message body
        """
        self._log_client.info("on_message method: {} properties: {} body: {}"
                              .format(basic_deliver, properties, body))

        self._consumed_messages.put(
            ConsumedMessage(body,
                            basic_deliver.exchange,
                            basic_deliver.routing_key,
                            properties.correlation_id if properties else None,
                            properties.reply_to if properties else None)
        )

        self._channel.basic_ack(basic_deliver.delivery_tag)

    def on_consume_ok(self, frame, consume):
        """
        Callback for when confirm mode has been activated.

        :param pika.spec.Method frame: message frame
        :param consume: issued consume
        """
        self._log_client.info("on_consume_ok frame: {}".format(frame))

        self._consumed_messages.put(
            ConsumeOk(frame.method.consumer_tag, consume)
        )
