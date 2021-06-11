import logging

from abc import ABC, abstractmethod

from threading import Thread, Timer

import pika
from pika import SelectConnection
from pika.exceptions import (
    ConnectionClosedByBroker,
    StreamLostError,
    ConnectionWrongStateError
)

from rabbitmq_client.defs import DEFAULT_EXCHANGE, PublishParams


LOGGER = logging.getLogger(__name__)

RECONNECT_REASONS = (
    ConnectionClosedByBroker,  # Restarts/graceful shutdowns
    StreamLostError  # Ungraceful shutdown
)


class RMQConnection(ABC):
    """
    Abstract base class to be implemented.

    Wraps a pika.SelectConnection and channel object to provide easy access to
    consumer/producer implementers. Allows for a quick start to writing
    consumers and producers, without having to re-write the connection logic.

    RMQConnection signals implementers with the on_ready and on_close methods
    when the connection/channel state changes.
    """

    def __init__(self, connection_parameters=None):
        """
        :param connection_parameters: pika.ConnectionParameters
        """
        # Kept public to allow restart with different parameters.
        self.connection_parameters: pika.ConnectionParameters = \
            connection_parameters

        self._connection_thread = Thread(target=self._connect,
                                         daemon=True)
        self._connection = None
        self._channel = None

        self._reconnect_attempts = 0

        self._closing = False
        self._restarting = False

    @abstractmethod
    def on_ready(self):
        """
        Implementers can handle what should happen when the RMQConnection is
        ready for work.
        """
        pass

    @abstractmethod
    def on_close(self, permanent=False):
        """
        Implementers can handle what should happen when the RMQConnection is
        suddenly closed. An on_close call will normally be followed by an
        on_ready call from the RMQConnection unless the connection is
        permanently down. It is possible that the RMQConnection will signal
        multiple on_close calls for a single connection failure, therefore the
        implementation of on_close must be idempotent.

        :param permanent: bool
        """
        pass

    @abstractmethod
    def on_error(self):
        """
        Implementers can handle what should happen when the RMQConnection has
        run into an error, for example an Exchange/Queue declaration failure.
        """
        pass

    def start(self):
        """
        Starts the connection, opens a connection and a channel by running the
        underlying connection thread. When ready, callbacks will be invoked on
        each step of the way towards establishing the connection and channel,
        starting with on_connection_open if all went well.

        NOTE! Starting the connection is not a synchronous operation! It is
        this way since starting may or may not be successful, so a return
        value would be necessary to indicate success/failure. But, since
        retries will be made, an initial failure may in fact be successful the
        next second, meaning the user is given faulty information. Start
        success is therefore indicated via the 'on_ready' hook instead.
        """
        if not self._connection_thread.is_alive():
            self._connection_thread.start()

    def restart(self):
        """
        Re-initiates the connection and channel.
        """
        LOGGER.info("restarting connection")

        try:
            self._connection.close()
            self._restarting = True  # Only if connection could be closed.
        except ConnectionWrongStateError:
            LOGGER.info("connection closed and could not be restarted")

    def stop(self):
        """
        Starts a closing procedure for the connection and channel.
        """
        LOGGER.info("stopping connection")

        if not self._closing:
            self._closing = True

            try:
                self._connection.close()
                self._connection_thread.join()
            except ConnectionWrongStateError:
                LOGGER.info("connection already closed")

    def declare_queue(self,
                      queue_params,
                      callback=None):
        """
        :param queue_params: rabbitmq_client.QueueParams
        :param callback: callable
        """
        LOGGER.info(f"declaring queue: {queue_params.queue}")

        self._channel.queue_declare(
            queue_params.queue,
            durable=queue_params.durable,
            exclusive=queue_params.exclusive,
            auto_delete=queue_params.auto_delete,
            arguments=queue_params.arguments,
            callback=callback
        )

    def declare_exchange(self,
                         exchange_params,
                         callback=None):
        """
        :param exchange_params: rabbitmq_client.ExchangeParams
        :param callback: callable
        """
        LOGGER.info(f"declaring exchange: {exchange_params.exchange}")

        self._channel.exchange_declare(
            exchange_params.exchange,
            exchange_type=exchange_params.exchange_type,
            durable=exchange_params.durable,
            auto_delete=exchange_params.auto_delete,
            internal=exchange_params.internal,
            arguments=exchange_params.arguments,
            callback=callback
        )

    def bind_queue(self,
                   queue_bind_params,
                   callback=None):
        """
        :param queue_bind_params: rabbitmq_client.QueueBindParams
        :param callback: callable
        """
        LOGGER.info(f"binding queue {queue_bind_params.queue} to exchange "
                    f"{queue_bind_params.exchange} with routing key: "
                    f"{queue_bind_params.routing_key}")

        self._channel.queue_bind(
            queue_bind_params.queue,
            queue_bind_params.exchange,
            routing_key=queue_bind_params.routing_key,
            arguments=queue_bind_params.arguments,
            callback=callback
        )

    def basic_consume(self,
                      consume_params,
                      on_message_callback_override=None,
                      callback=None):
        """
        :param consume_params: rabbitmq_client.ConsumeParams
        :param on_message_callback_override: callable
        :param callback: callable
        """
        LOGGER.info(f"basic consumer starting for queue: "
                    f"{consume_params.queue}")

        self._channel.basic_consume(
            consume_params.queue,
            (on_message_callback_override if on_message_callback_override
             else consume_params.on_message_callback),
            auto_ack=consume_params.auto_ack,
            exclusive=consume_params.exclusive,
            consumer_tag=consume_params.consumer_tag,
            arguments=consume_params.arguments,
            callback=callback
        )

    def basic_publish(self,
                      body,
                      exchange=DEFAULT_EXCHANGE,
                      routing_key="",
                      publish_params=None):
        """
        :param body: bytes
        :param exchange: str
        :param routing_key: str
        :param publish_params: rabbitmq_client.PublishParams
        """
        LOGGER.debug(f"publishing to exchange: {exchange} and routing key: "
                     f"{routing_key}")

        if not publish_params:
            publish_params = PublishParams()  # Create defaults

        self._channel.basic_publish(exchange,
                                    routing_key,
                                    body,
                                    properties=publish_params.properties,
                                    mandatory=publish_params.mandatory)

    def confirm_delivery(self, on_delivery_confirmed, callback=None):
        """
        :param on_delivery_confirmed: callable
        :param callback: callable
        """
        LOGGER.info("activating confirm delivery mode")

        self._channel.confirm_delivery(on_delivery_confirmed,
                                       callback=callback)

    def _connect(self):
        """
        Assigns a new pika.SelectConnection to the connection and starts its
        ioloop to begin connecting.
        """
        if self.connection_parameters is not None:
            LOGGER.info(f"starting connection towards: "
                        f"{self.connection_parameters.host}:"
                        f"{self.connection_parameters.port}")
        else:
            LOGGER.info("starting connection towards: 127.0.0.1:5672")

        self._connection = SelectConnection(
            parameters=self.connection_parameters,
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed
        )
        self._connection.ioloop.start()

    def on_connection_open(self, _connection):
        """
        Called when a new connection has been established. Opens a new channel.

        :param _connection: pika.SelectConnection
        """
        LOGGER.debug("connection opened")
        # Do NOT reset reconnect counter here, do it after channel opened.

        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_connection_open_error(self, _connection, error):
        """
        Called when a connection could not be established towards RabbitMQ.
        This will lead to attempts to reconnect with an increasing delay
        between attempts.

        :param _connection: pika.SelectConnection
        :param error: pika.exceptions.?
        """
        LOGGER.warning(f"error establishing connection, error: {error}")

        # This should ensure the current thread runs to completion after
        # reconnect handling is done.
        self._connection.ioloop.stop()

        # No fancy handling, just retry even if credentials are bad.
        self._reconnect()

    def on_connection_closed(self, _connection, reason):
        """
        Called when the connection is closed, either unexpectedly or on
        purpose. Reconnection is attempted if the disconnect reason is
        something that is possible to recover, like an unexpected broker
        shutdown.

        :param _connection: pika.SelectConnection
        :param reason: pika.exceptions.?
        """
        permanent = False

        # This should ensure the current thread runs to completion after
        # on_connection_closed handling is done.
        self._connection.ioloop.stop()

        if not self._closing and type(reason) in RECONNECT_REASONS:
            LOGGER.debug(f"connection closed: {reason}, attempting reconnect")
            self._reconnect()

        elif self._restarting:
            LOGGER.debug("connection closed, restarting now")
            # Reset to prevent reconnect for any reason
            self._restarting = False
            self._connection_thread = Thread(target=self._connect,
                                             daemon=True)
            self._connection_thread.start()

        else:
            permanent = True
            LOGGER.warning(f"connection closed: {reason}, will not reconnect")

        # Signal subclass that connection is down.
        self.on_close(permanent=permanent)

    def _reconnect(self):
        """
        Starts up the connection again after a gradually increasing delay.
        """
        LOGGER.debug(f"reconnect, attempt no. {self._reconnect_attempts + 1}")

        # Reconnect attempt may have been queued up before 'stop' was called.
        if self._closing:
            LOGGER.debug("skipping reconnect, connection stopped")
            return

        # _connect will assign a new connection object
        self._connection_thread = Thread(target=self._connect,
                                         daemon=True)
        if self._reconnect_attempts == 0:  # retry immediately the first time
            self._connection_thread.start()

        else:  # calculate backoff timer
            timer = Timer(
                self._reconnect_attempts if self._reconnect_attempts < 9
                else 30,
                self._connection_thread.start
            )
            # Timer daemon exits if program exits, to avoid eternal retries.
            timer.daemon = True
            timer.start()

        self._reconnect_attempts += 1

    def on_channel_open(self, channel):
        """
        Called when a new channel has been opened, the consumer is now ready
        for work.

        :param channel: pika.channel.Channel
        """
        LOGGER.debug("channel opened")
        self._reconnect_attempts = 0

        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)

        self.on_ready()  # Signal subclass that connection is ready.

    def on_channel_closed(self, _channel, reason):
        """
        Called when a channel is closed by RabbitMQ. Channels are not
        attempted to be re-opened since they are either closed from going down
        together with the connection, or because input parameters for some
        queue/exchange/other establishment was given faulty parameters.
        Retrying such an operation will just result in a failure loop.

        :param _channel: pika.channel.Channel
        :param reason: pika.exceptions.?
        """
        LOGGER.warning("channel closed")

        permanent = False

        try:
            # Reply code 406 = PRECONDITION FAILED
            permanent = True if reason.reply_code == 406 else False
        except AttributeError:  # Not all reasons have a reply code
            pass

        # Signal subclass that connection is down.
        self.on_close(permanent=permanent)

        if permanent:
            LOGGER.critical(f"connection stopping due to permanent channel "
                            f"closure: {reason}")
            self.stop()
