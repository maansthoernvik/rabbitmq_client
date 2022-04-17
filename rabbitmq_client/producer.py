import functools
import logging
import uuid

from typing import Callable, Union
from threading import Lock

from pika.spec import Basic

from rabbitmq_client.connection import (
    RMQConnection,
    MandatoryError,
    DeclarationError
)
from rabbitmq_client.defs import (
    ConfirmModeOK,
    DeliveryError,
    DEFAULT_EXCHANGE,
    QueueParams,
    ExchangeParams,
    PublishParams
)

LOGGER = logging.getLogger(__name__)


class RMQPublish:

    def __init__(self,
                 body,
                 exchange_params=None,
                 routing_key="",
                 queue_params=None,
                 publish_params=None,
                 publish_key=None):
        """
        :param body: bytes
        :param exchange_params: rabbitmq_client.ExchangeParams
        :param routing_key: str
        :param queue_params: rabbitmq_client.QueueParams
        :param publish_params: rabbitmq_client.PublishParams
        :param publish_key: str
        """
        self.body = body
        self.exchange_params = exchange_params
        self.routing_key = routing_key
        self.queue_params = queue_params
        self.publish_params = publish_params
        self.publish_key = publish_key


class RMQProducer(RMQConnection):
    """
    Generic producer implementation using the RMQConnection base class to ease
    connection and channel handling.
    """

    def __init__(self, connection_parameters=None):
        """
        :param connection_parameters: pika.ConnectionParameters
        """
        super().__init__(connection_parameters=connection_parameters)

        self._ready = False

        self._buffered_messages = list()

        # Confirm mode
        self._confirm_mode_active = False
        self._confirm_delivery_callback = None
        self._next_delivery_tag = 1
        self._publish_lock = Lock()
        self._unacked_publishes = dict()

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
        LOGGER.info("starting producer")

        super().start()

    def restart(self):
        """Restarts the consumer by re-initiating the underlying connection."""
        LOGGER.info("restarting producer")

        super().restart()

    def stop(self):
        """Stops the consumer by stopping the underlying connection."""
        LOGGER.info("stopping producer")

        super().stop()

    def publish(self,
                body: bytes,
                publish_params: PublishParams = None,
                queue_params: QueueParams = None,
                exchange_params: ExchangeParams = None,
                routing_key: str = "") -> Union[None, str]:
        """
        Publish 'body' towards an exchange (optional routing key) or a queue.
        You must provide EITHER exchange_params OR queue_params to handle
        the declaration of the publish target, but not both.

        The reasoning here is that a publisher may produce a message to an
        exchange, but is not responsible for declaring the end-queue and its
        binding. So, publish only declares the entity to which it produces its
        message(s) but should not know all the details of an exchange-bound
        queues properties. For work queues, the producer of course needs to
        know declaration details since there is no intermediate step to publish
        to.

        If confirm mode is activated, a key is returned which corresponds to
        the message being published. Users can confirm that their publish has
        been sent successfully by waiting for their key to be referred to in
        a call to their confirm mode notify callback where the input parameter
        == the generated publish key.
        """
        # More detailed logging done on connection level
        LOGGER.info("publishing")

        if (
                (not exchange_params and not queue_params) or
                (exchange_params and queue_params)
        ):
            raise ValueError(
                "You need to provide either a queue OR an exchange, else "
                "there is nothing to publish to..."
            )

        publish_key = None
        if self._confirm_delivery_callback is not None:
            publish_key = str(uuid.uuid4())

        if (
                self.ready and
                (
                    (self._confirm_delivery_callback is not None and
                     self._confirm_mode_active) or
                    self._confirm_delivery_callback is None
                )
        ):
            self._handle_publish(body,
                                 publish_params=publish_params,
                                 queue_params=queue_params,
                                 exchange_params=exchange_params,
                                 routing_key=routing_key,
                                 publish_key=publish_key)
        else:
            self._buffered_messages.append(
                RMQPublish(body,
                           exchange_params=exchange_params,
                           routing_key=routing_key,
                           queue_params=queue_params,
                           publish_params=publish_params,
                           publish_key=publish_key)
            )

        return publish_key

    def activate_confirm_mode(self,
                              notify_callback: Callable[
                                  [Union[str, ConfirmModeOK, DeliveryError]],
                                  None
                              ]):
        if self.ready and self._confirm_delivery_callback is None:
            self.confirm_delivery(self.on_delivery_confirmed,
                                  callback=self.on_confirm_select_ok)

        self._confirm_delivery_callback = notify_callback

    def _handle_publish(self,
                        body: bytes,
                        publish_params: PublishParams = None,
                        queue_params: QueueParams = None,
                        exchange_params: ExchangeParams = None,
                        routing_key: str = "",
                        publish_key: Union[None, str] = None):
        if queue_params:
            cb = functools.partial(self.when_queue_declared,
                                   body,
                                   publish_params=publish_params,
                                   publish_key=publish_key)
            self.declare_queue(queue_params, cb)
        else:
            cb = functools.partial(self.when_exchange_declared,
                                   body,
                                   exchange_params,
                                   routing_key,
                                   publish_params=publish_params,
                                   publish_key=publish_key)
            self.declare_exchange(exchange_params, cb)

    def when_queue_declared(self,
                            body: bytes,
                            queue_name: str,
                            publish_params: PublishParams = None,
                            publish_key: Union[None, str] = None):
        self._finalize_publish(body,
                               routing_key=queue_name,
                               publish_params=publish_params,
                               publish_key=publish_key)

    def when_exchange_declared(self,
                               body: bytes,
                               exchange_params: ExchangeParams,
                               routing_key: str,
                               publish_params: PublishParams = None,
                               publish_key: Union[None, str] = None):
        self._finalize_publish(body,
                               exchange=exchange_params.exchange,
                               routing_key=routing_key,
                               publish_params=publish_params,
                               publish_key=publish_key)

    def _finalize_publish(self,
                          body,
                          exchange: str = DEFAULT_EXCHANGE,
                          routing_key: str = "",
                          publish_params: PublishParams = None,
                          publish_key: Union[None, str] = None):
        if publish_key is not None:
            with self._publish_lock:
                LOGGER.debug(f"delivering with tag {self._next_delivery_tag}")

                self._unacked_publishes[self._next_delivery_tag] = publish_key
                self._next_delivery_tag += 1

                self.basic_publish(body,
                                   exchange,
                                   routing_key,
                                   publish_params=publish_params)
        else:
            self.basic_publish(body,
                               exchange,
                               routing_key,
                               publish_params=publish_params)

    def _empty_buffered_messages(self):
        """
        Sends all buffered messages.
        """
        for buffered_message in self._buffered_messages:
            self._handle_publish(
                buffered_message.body,
                publish_params=buffered_message.publish_params,
                queue_params=buffered_message.queue_params,
                exchange_params=buffered_message.exchange_params,
                routing_key=buffered_message.routing_key,
                publish_key=buffered_message.publish_key
            )

        self._buffered_messages = list()

    def on_confirm_select_ok(self, _frame):
        """
        :param _frame: pika.frame.Method
        """
        LOGGER.info("confirm select ok")
        self._confirm_mode_active = True
        self._confirm_delivery_callback(ConfirmModeOK())

        self._empty_buffered_messages()

    def on_delivery_confirmed(self, frame):
        """
        :param frame: pika.frame.Method
        """
        LOGGER.debug(f"a delivery was confirmed: {frame.method.delivery_tag}")

        try:
            publish_key = self._unacked_publishes.pop(
                frame.method.delivery_tag
            )

            if isinstance(frame.method, Basic.Ack):
                self._confirm_delivery_callback(publish_key)

            else:
                LOGGER.error(f"broker nacked a publish: {frame}")
                self._confirm_delivery_callback(
                    DeliveryError(publish_key)
                )
        except KeyError:
            LOGGER.warning(f"RabbitMQ confirmed unexpected delivery tag:"
                           f" {frame.method.delivery_tag}")

    def on_ready(self):
        """
        Connection hook, called when channel opened, meaning RMQConnection is
        ready for work.
        """
        LOGGER.info("producer connection ready")

        self._ready = True

        if self._confirm_delivery_callback is not None:
            self.confirm_delivery(self.on_delivery_confirmed,
                                  callback=self.on_confirm_select_ok)

        else:
            self._empty_buffered_messages()

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
            LOGGER.critical("producer connection permanently closed")
        else:
            LOGGER.info("producer connection closed")

        self._ready = False
        self._confirm_mode_active = False
        # Delivery tags are channel-specific, so connection going down means
        # the tag is reset.
        self._next_delivery_tag = 1

    def on_error(self, error: Union[MandatoryError, DeclarationError]):
        """
        Connection hook, called when the connection has encountered an error.
        """
        LOGGER.info("producer connection error")

        if isinstance(error, MandatoryError):
            if self._confirm_delivery_callback is not None:
                self._confirm_delivery_callback(error)
            else:
                LOGGER.error(f"failed to publish to "
                             f"'{error.exchange}' + '{error.routing_key}', "
                             f"no queue is bound to it")

        elif isinstance(error, DeclarationError):
            LOGGER.error(f"failed to declare something: {error.message}")
