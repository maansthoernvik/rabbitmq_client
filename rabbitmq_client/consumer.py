from typing import Union

import functools
import logging

from rabbitmq_client.defs import (
    QueueParams,
    QueueBindParams,
    ConsumeOK,
    ConsumeParams,
    ExchangeParams
)
from rabbitmq_client.connection import RMQConnection


LOGGER = logging.getLogger(__name__)


def _gen_consume_key(queue: str = None,
                     exchange: str = None,
                     routing_key: str = None) -> str:
    key_list = []
    key_list.append(queue) if queue else ""
    key_list.append(exchange) if exchange else ""
    key_list.append(routing_key) if routing_key else ""

    separator = "|"

    return separator.join(key_list)


class RMQConsume:
    """
    Used by the RMQConsumer to keep track of which consumes have been started
    so far, and to be able to re-distribute consumes should the underlying
    connection experience a hiccup.
    """

    def __init__(self,
                 consume_params,
                 queue_params,
                 exchange_params,
                 routing_key):
        """
        :param consume_params: rabbitmq_client.ConsumeParams
        :param queue_params: rabbitmq_client.QueueParams
        :param exchange_params: rabbitmq_client.ExchangeParams
        :param routing_key: str
        """
        self.consume_params = consume_params
        self.queue_params = queue_params
        self.exchange_params = exchange_params
        self.routing_key = routing_key

        # The actual consumer tag from 'basic_consume'. This will be equal to
        # 'self.consume_params.consumer_tag' if one was set by the caller,
        # otherwise it is randomly generated by pika. It is used to cancel
        # consumes with 'basic_cancel'.
        self._consumer_tag = None

    @property
    def consumer_tag(self):
        return self._consumer_tag

    @consumer_tag.setter
    def consumer_tag(self, new_value):
        self._consumer_tag = new_value


class RMQConsumer(RMQConnection):
    """
    Generic consumer implementation using the RMQConnection base class to ease
    connection and channel handling.

    The 'consume' method provided by this consumer implementation accepts
    parameters as they will be passed to the pika library to avoid going out
    of its way to create another layer of special handling. A few parameters
    have been removed, but nothing has been added.
    """

    def __init__(self, connection_parameters=None):
        """
        :param connection_parameters: pika.ConnectionParameters
        """
        super().__init__(connection_parameters=connection_parameters)

        self._ready = False
        self._consumes = dict()

        self._declared_queues = set()
        self._declared_exchanges = set()

    @property
    def ready(self):
        """
        Indicates if the consumer is ready, meaning it will immediately issue
        consume-work it receives. If the consumer is NOT ready, incoming
        consumes will be delayed until the underlying connection reports ready
        through the 'on_ready' hook.

        :return: bool
        """
        return self._ready

    def start(self):
        """
        Starts the consumer by initiating the underlying connection.

        NOTE! This is NOT a synchronous operation! If you must know that the
        consumer is successfully started, monitor the 'ready' property.
        """
        LOGGER.info("starting consumer")

        super().start()

    def restart(self):
        """Restarts the consumer by re-initiating the underlying connection."""
        LOGGER.info("restarting consumer")

        super().restart()

    def stop(self):
        """Stops the consumer by stopping the underlying connection."""
        LOGGER.info("stopping consumer")

        super().stop()

    def consume(self,
                consume_params,
                queue_params=None,
                exchange_params=None,
                routing_key=None):
        """
        General consumer interface, when wanting to consumer messages sent to a
        specific queue or exchange. Input parameters are a subset of those used
        by pika, to avoid re-inventing the wheel with special abstractions.
        The only parameters that have been removed are the "passive" and
        "callback" options.

        Some restrictions apply:
        1. EITHER queue OR exchange OR both MUST be set.
        2. Queue/exchange parameter change is not supported. For example, this
           consumer will not handle a re-declaration of a queue with new
           provided parameters.

        RMQConsumer will automatically re-issue queue and exchange declarations
        and bindings if the connection is for some reason closed and re-opened
        at any point.

        This method may be called before the underlying RMQConnection has
        successfully established a connection and channel to RabbitMQ. Those
        buffered actions called before RMQConnection reports it is ready,
        through the 'on_ready' hook, will be issued immediately upon 'on_ready'
        being called. Monitor the RMQConsumer 'ready' property to keep track of
        when the RMQConnection has reported it is ready.

        :param consume_params: rabbitmq_client.ConsumeParams
        :param queue_params: rabbitmq_client.QueueParams
        :param exchange_params: rabbitmq_client.ExchangeParams
        :param routing_key: str
        :returns: str
        """
        LOGGER.info("starting consume")

        # 1. Checks
        if queue_params is None and exchange_params is None:
            raise ValueError(
                "You need to provide either a queue, an exchange, or both, "
                "else there is nothing to consume from..."
            )

        consume_key = _gen_consume_key(
            queue_params.queue if queue_params else "",
            exchange_params.exchange if exchange_params else "",
            routing_key
        )
        if self._consumes.get(consume_key) is not None:
            raise ValueError(
                "That combination of queue + exchange + routing key is "
                "already consumed from."
            )

        # 2. Update consumer instance
        self._consumes[consume_key] = RMQConsume(
            consume_params, queue_params, exchange_params, routing_key,
        )

        # 3. Start declaring shit
        if self.ready:
            self._handle_consume(consume_params,
                                 queue_params,
                                 exchange_params,
                                 routing_key)

        return consume_key

    def _handle_consume(self,
                        consume_params,
                        queue_params,
                        exchange_params,
                        routing_key):
        """
        :param consume_params: rabbitmq_client.ConsumeParams
        :param queue_params: None | rabbitmq_client.QueueParams
        :param exchange_params: None | rabbitmq_client.ExchangeParams
        :param routing_key: None | str
        """
        if queue_params is None:
            queue_params = QueueParams("", exclusive=True)

        if queue_params.queue in self._declared_queues:
            self.check_declare_exchange(consume_params,
                                        queue_params,
                                        exchange_params,
                                        routing_key)

        else:
            cb = functools.partial(self.on_queue_declared,
                                   consume_params=consume_params,
                                   queue_params=queue_params,
                                   exchange_params=exchange_params,
                                   routing_key=routing_key)

            self.declare_queue(queue_params, callback=cb)

    def on_queue_declared(self,
                          frame,
                          consume_params=None,
                          queue_params=None,
                          exchange_params=None,
                          routing_key=None):
        """
        :param frame: pika.frame.Method
        :param consume_params: rabbitmq_client.ConsumeParams
        :param queue_params: rabbitmq_client.QueueParams
        :param exchange_params: rabbitmq_client.ExchangeParams
        :param routing_key: str
        """
        LOGGER.info(f"declared queue: {frame.method.queue}")

        self._declared_queues.add(frame.method.queue)

        # Update the consume queue name to ensure it is set to the created
        # queue's name
        consume_params.queue = frame.method.queue

        self.check_declare_exchange(consume_params,
                                    queue_params,
                                    exchange_params,
                                    routing_key)

    def check_declare_exchange(self,
                               consume_params: ConsumeParams,
                               queue_params: QueueParams,
                               exchange_params: Union[ExchangeParams, None],
                               routing_key: str):
        if (
                exchange_params is not None and
                exchange_params.exchange in self._declared_exchanges
        ):
            self.handle_queue_binding(consume_params,
                                      queue_params,
                                      exchange_params,
                                      routing_key)

        elif exchange_params is not None:
            cb = functools.partial(self.on_exchange_declared,
                                   exchange_params,
                                   consume_params=consume_params,
                                   queue_params=queue_params,
                                   routing_key=routing_key)

            self.declare_exchange(exchange_params, callback=cb)

        else:
            cb = functools.partial(self.on_consume_ok,
                                   queue_params=queue_params)

            self.basic_consume(consume_params,
                               on_message_callback_override=self.on_msg,
                               callback=cb)

    def on_exchange_declared(self,
                             exchange_params,
                             _frame,
                             consume_params=None,
                             queue_params=None,
                             routing_key=None):
        """
        :param exchange_params: rabbitmq_client.ExchangeParams
        :param _frame: pika.frame.Method
        :param consume_params: rabbitmq_client.ConsumeParams
        :param queue_params: rabbitmq_client.QueueParams
        :param routing_key: str
        """
        LOGGER.info(f"declared exchange: {exchange_params.exchange}")

        self._declared_exchanges.add(exchange_params.exchange)

        self.handle_queue_binding(consume_params,
                                  queue_params,
                                  exchange_params,
                                  routing_key)

    def handle_queue_binding(self,
                             consume_params: ConsumeParams,
                             queue_params: QueueParams,
                             exchange_params: ExchangeParams,
                             routing_key: str):
        cb = functools.partial(self.on_queue_bound,
                               consume_params=consume_params,
                               queue_params=queue_params,
                               exchange=exchange_params.exchange,
                               routing_key=routing_key)

        self.bind_queue(QueueBindParams(consume_params.queue,
                                        exchange_params.exchange,
                                        routing_key=routing_key),
                        callback=cb)

    def on_queue_bound(self,
                       _frame,
                       consume_params=None,
                       queue_params=None,
                       exchange=None,
                       routing_key=None):
        """
        :param _frame: pika.frame.Method
        :param consume_params: rabbitmq_client.ConsumeParams
        :param queue_params: rabbitmq_client.QueueParams
        :param exchange: str
        :param routing_key: str
        """
        LOGGER.info(f"queue {consume_params.queue} bound to exchange "
                    f"{exchange}")

        cb = functools.partial(self.on_consume_ok,
                               queue_params=queue_params,
                               exchange=exchange,
                               routing_key=routing_key)

        self.basic_consume(consume_params,
                           on_message_callback_override=self.on_msg,
                           callback=cb)

    def on_consume_ok(self,
                      frame,
                      queue_params=None,
                      exchange=None,
                      routing_key=None):
        """
        :param frame: pika.frame.Method
        :param queue_params: rabbitmq_client.QueueParams
        :param exchange: str
        :param routing_key: str
        """
        LOGGER.info(f"consume OK for queue: {queue_params.queue}")

        consume_instance = self._consumes[
            _gen_consume_key(queue=queue_params.queue,
                             exchange=exchange,
                             routing_key=routing_key)
        ]
        # Update with real consumer tag, may or may not be the same tag.
        consume_instance.consumer_tag = frame.method.consumer_tag

        # Enables lookup via consumer tag in 'on_msg'. These entries are
        # removed 'on_close' since the consumer tags may be refreshed on
        # reconnecting.
        self._consumes[consume_instance.consumer_tag] = consume_instance

        try:
            consume_instance.consume_params.on_message_callback(
                ConsumeOK(consume_instance.consumer_tag)
            )
        except Exception as e:
            LOGGER.critical(f"sending consume OK to message callback resulted "
                            f"in an exception: {e}")

    def on_msg(self, channel, basic_deliver, _basic_properties, body):
        """
        :param channel: pika.channel.Channel
        :param basic_deliver: pika.spec.Basic.Deliver
        :param _basic_properties: pika.spec.Basic.Properties
        :param body: bytes
        """
        consume = self._consumes[basic_deliver.consumer_tag]

        try:
            if not consume.consume_params.auto_ack:
                consume.consume_params.on_message_callback(
                    body,
                    ack=lambda:
                        channel.basic_ack(
                            delivery_tag=basic_deliver.delivery_tag
                        )
                )
            else:
                consume.consume_params.on_message_callback(body)
        except Exception as e:
            LOGGER.warning(f"the on_message_callback for queue: "
                           f"{consume.consume_params.queue} "
                           f"crashed with error: {e}")

    def on_ready(self):
        """
        Connection hook, called when channel opened, meaning RMQConnection is
        ready for work.
        """
        LOGGER.info("consumer connection ready")

        self._ready = True

        for _key, consume in self._consumes.items():
            self._handle_consume(consume.consume_params,
                                 consume.queue_params,
                                 consume.exchange_params,
                                 consume.routing_key)

    def on_close(self, permanent=False):
        """
        Connection hook, called when the channel or connection is closed.

        NOTE!
        This is NOT reported as an error because users are expected to
        configure their exchanges and queues accordingly to avoid losing data
        they cannot live without.

        :param permanent: bool
        """
        if not self._closing and permanent:
            LOGGER.critical("consumer connection permanently closed")
        else:
            LOGGER.info("consumer connection closed")

        self._ready = False

        # Reset cached declarations, no way of knowing what's still around
        # after a restart.
        self._declared_queues = set()
        self._declared_exchanges = set()

        old_consumer_tags = list()
        for _key, consume in self._consumes.items():
            # Remove all consumer tag keys in local dict since these may change
            # on re-subscription.
            if consume.consumer_tag is not None:
                old_consumer_tags.append(consume.consumer_tag)
            consume.consumer_tag = None

        for consumer_tag in old_consumer_tags:
            self._consumes.pop(consumer_tag, None)

    def on_error(self):
        """
        Connection hook, called when the connection has encountered an error.
        """
        LOGGER.info("consumer connection error")

        raise NotImplementedError
        # Possible errors:
        # * channel died and will not recover
        # * callback for operation failed
        # * declaration with faulty parameters attempted
        # TODO: Add possibility to signal user that an error has occurred.
