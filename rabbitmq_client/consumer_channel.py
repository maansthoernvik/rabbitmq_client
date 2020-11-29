import functools
import logging

from .consumer_defs import Subscription, RPCServer, RPCClient, \
                           ConsumedMessage, ConsumeOk, CommandQueue, \
                           AUTO_GEN_QUEUE_NAME
from .common_defs import EXCHANGE_TYPE_FANOUT


LOGGER = logging.getLogger(__name__)


class RMQConsumerChannel:
    """
    Defines handling of a consumer channel. Declarations of queues and
    exchanges, consuming and error handling.
    """

    def __init__(self, consumed_messages):
        """
        :param consumed_messages: IPC queue used to relay consumed messages
                                  between processes
        """
        LOGGER.debug("__init__")

        self._consumed_messages = consumed_messages

        self._channel = None
        self._open = False

    def open_channel(self, connection, notify_callback):
        """
        Opens a channel for the parameter connection.

        :param connection: current connection object
        :param notify_callback: callback to notify once channel is ready for
                                work
        """
        LOGGER.debug("open_channel")

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
        LOGGER.info("on_channel_open channel: {}".format(channel))

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
        LOGGER.info(f"on_channel_closed channel: {_channel} reason: {reason}")

        self._open = False

    def handle_consume(self, consume):
        """
        Initiates a new consumer for the channel.

        :param consume: consumer information needed to establish the new
                        consumer
        """
        LOGGER.debug(f"handle_consume consume: {consume}")

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
                                        durable=True,  # Survive broker reboot
                                        callback=cb)

        elif isinstance(consume, RPCClient):
            cb = functools.partial(self.on_queue_declared,
                                   consume=consume)
            self._channel.queue_declare(queue=consume.queue_name,
                                        exclusive=True,
                                        callback=cb)

        elif isinstance(consume, CommandQueue):
            cb = functools.partial(self.on_queue_declared,
                                   consume=consume)
            self._channel.queue_declare(queue=consume.queue_name,
                                        durable=True,  # Survive broker reboot
                                        callback=cb)

    def on_exchange_declared(self, _frame, consume):
        """
        Callback for when an exchange has been declared.

        :param pika.frame.Method _frame: message frame
        :param consume: consumer information needed to establish the new
                        consumer
        """
        LOGGER.debug("on_exchange_declared frame: {} consume: {}"
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
        LOGGER.debug(f"on_queue_declared frame: {frame} consume: {consume}")

        if isinstance(consume, Subscription):
            consume.set_queue_name(frame.method.queue)
            cb = functools.partial(self.on_queue_bound,
                                   consume=consume)
            self._channel.queue_bind(
                consume.queue_name, consume.topic, callback=cb
            )

        elif isinstance(consume, RPCServer) or \
                isinstance(consume, RPCClient) or \
                isinstance(consume, CommandQueue):
            # No exchange = no need to bind the queue, can go ahead and consume
            # immediately.
            self.consume(consume)

    def on_queue_bound(self, _frame, consume):
        """
        Callback for when a queue has been bound to an exchange.

        :param pika.frame.Method _frame: message frame
        :param consume: consumer information needed to establish the new
                        consumer
        """
        LOGGER.debug("on_queue_bound frame: {} consume: {}"
                     .format(_frame, consume))

        self.consume(consume)

    def consume(self, consume):
        """
        Starts consuming on the parameter queue.

        :param consume: consume action
        """
        LOGGER.info("consume queue_name: {}"
                    .format(consume.queue_name))

        cb = functools.partial(self.on_consume_ok,
                               consume=consume)
        # All consumes so far are exclusive, meaning there can be only one
        # consumer for the given queue.
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
        LOGGER.info(f"on_message method: {basic_deliver} properties: "
                    f"{properties} body: {body}")

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
        LOGGER.info("on_consume_ok frame: {}".format(frame))

        self._consumed_messages.put(
            ConsumeOk(frame.method.consumer_tag, consume)
        )
