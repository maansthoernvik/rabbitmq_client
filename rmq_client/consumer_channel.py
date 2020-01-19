import functools
import logging

from multiprocessing import Queue as IPCQueue

from .consumer_defs import Subscription, RPCServer, RPCClient, ConsumedMessage
from .common_defs import AUTO_GEN_QUEUE_NAME, EXCHANGE_TYPE_FANOUT
from .log import LogItem


class RMQConsumerChannel:

    _log_queue: IPCQueue

    _channel = None

    _consumed_messages: IPCQueue

    _open = False

    def __init__(self, consumed_messages, log_queue):
        self._log_queue = log_queue
        self._log_queue.put(
            LogItem("__init__", RMQConsumerChannel.__name__,
                    level=logging.DEBUG)
        )

        self._consumed_messages = consumed_messages

    def open_channel(self, connection, notify_callback):
        self._log_queue.put(
            LogItem("open_channel", RMQConsumerChannel.__name__,
                    level=logging.DEBUG)
        )

        cb = functools.partial(self.on_channel_open,
                               notify_callback=notify_callback)

        if not self._open:
            connection.channel(on_open_callback=cb)

    def on_channel_open(self, channel, notify_callback=None):
        self._log_queue.put(
            LogItem("on_channel_open channel: {}".format(channel),
                    RMQConsumerChannel.__name__, level=logging.INFO)
        )

        self._open = True

        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)

        notify_callback()

    def on_channel_closed(self, _channel, reason):
        self._log_queue.put(
            LogItem("on_channel_closed channel: {} reason: {}"
                    .format(_channel, reason),
                    RMQConsumerChannel.__name__,
                    level=logging.INFO)
        )

        self._open = False

    def handle_consume(self, consume):
        self._log_queue.put(
            LogItem("handle_consume consume: {}".format(consume),
                    RMQConsumerChannel.__name__,
                    level=logging.DEBUG)
        )

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
        :param str exchange_name: additional parameter from functools.partial,
                                  used to carry the exchange_name
        """
        self._log_queue.put(
            LogItem("on_exchange_declared consume: {}, frame: {}"
                    .format(consume, _frame),
                    RMQConsumerChannel.__name__,
                    level=logging.DEBUG)
        )
        cb = functools.partial(self.on_queue_declared,
                               consume=consume)
        self._channel.queue_declare(queue=AUTO_GEN_QUEUE_NAME,
                                    exclusive=True,
                                    callback=cb)

    def on_queue_declared(self, frame, consume):
        """
        Callback for when a queue has been declared.

        :param pika.frame.Method frame: message frame
        :param str exchange_name: additional parameter from functools.partial,
                                  used to carry the exchange_name
        """
        self._log_queue.put(
            LogItem("on_queue_declared consume: {}, frame: {}"
                    .format(consume, frame),
                    RMQConsumerChannel.__name__,
                    level=logging.DEBUG)
        )

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
            self.consume(frame.method.queue)

    def on_queue_bound(self, _frame, consume):
        """
        Callback for when a queue has been bound to an exchange.

        :param pika.frame.Method _frame: message frame
        """
        self._log_queue.put(
            LogItem("on_queue_bound consume: {} "
                    "frame: {}".format(consume, _frame),
                    RMQConsumerChannel.__name__, level=logging.DEBUG)
        )
        self.consume(consume.queue_name)

    def consume(self, queue_name):
        """
        Starts consuming on the parameter queue.

        :param str queue_name: name of the queue to consume from
        """
        self._log_queue.put(
            LogItem("consume queue_name: {}".format(queue_name),
                    RMQConsumerChannel.__name__,
                    level=logging.DEBUG)
        )

        # All consumes so far are exclusive.
        self._channel.basic_consume(queue_name, self.on_message, exclusive=True)

    def on_message(self, _channel, basic_deliver, properties, body):
        """
        Callback for when a message is received on a consumed queue.

        :param _channel: channel that the message was received on
        :param pika.spec.Basic.Deliver basic_deliver: method
        :param pika.spec.BasicProperties properties: properties of the message
        :param bytes body: message body
        """
        self._log_queue.put(
            LogItem("on_message method: {} properties: {} body: {}"
                    .format(basic_deliver, properties, body),
                    RMQConsumerChannel.__name__)
        )

        self._consumed_messages.put(
            ConsumedMessage(body,
                            basic_deliver.exchange,
                            basic_deliver.routing_key,
                            properties.correlation_id if properties else None,
                            properties.reply_to if properties else None)
        )

        self._channel.basic_ack(basic_deliver.delivery_tag)
