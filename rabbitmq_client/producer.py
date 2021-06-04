import functools
import logging

from rabbitmq_client.connection import RMQConnection


LOGGER = logging.getLogger(__name__)


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
                body,
                exchange_params=None,
                routing_key="",
                queue_params=None,
                publish_params=None):
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

        :param body: bytes
        :param exchange_params: rabbitmq_client.ExchangeParams
        :param routing_key: str
        :param queue_params: rabbitmq_client.QueueParams
        :param publish_params: rabbitmq_client.PublishParams
        """
        LOGGER.info("publishing")
        LOGGER.debug(f"exchange: {exchange_params}, routing_key: "
                     f"{routing_key}, queue: {queue_params}")

        if (
                (not exchange_params and not queue_params) or
                (exchange_params and queue_params)
        ):
            raise ValueError(
                "You need to provide either a queue OR an exchange, else "
                "there is nothing to publish to..."
            )

        if self.ready:
            self._handle_publish(body,
                                 exchange_params,
                                 routing_key,
                                 queue_params,
                                 publish_params)

    def _handle_publish(self,
                        body,
                        exchange_params,
                        routing_key,
                        queue_params,
                        publish_params):
        """
        :param body: bytes
        :param exchange_params: rabbitmq_client.ExchangeParams
        :param routing_key: str
        :param queue_params: rabbitmq_client.QueueParams
        :param publish_params: rabbitmq_client.PublishParams
        """
        if queue_params:
            cb = functools.partial(self.on_queue_declared,
                                   body,
                                   publish_params=publish_params)

            self.declare_queue(queue_params, callback=cb)

        else:
            cb = functools.partial(self.on_exchange_declared,
                                   body,
                                   routing_key=routing_key,
                                   publish_params=publish_params)

            self.declare_exchange(exchange_params, callback=cb)

    def on_queue_declared(self,
                          body,
                          frame,
                          publish_params=None):
        """
        :param body: bytes
        :param frame: pika.frame.Method
        :param publish_params: rabbitmq_client.PublishParams
        """
        LOGGER.info(f"declared queue: {frame.method.queue}")

        self.basic_publish(body,
                           routing_key=frame.method.queue,
                           publish_params=publish_params)

    def on_exchange_declared(self,
                             body,
                             frame,
                             routing_key="",
                             publish_params=None):
        """
        :param body: bytes
        :param frame: pika.frame.Method
        :param routing_key: str
        :param publish_params: rabbitmq_client.PublishParams
        """
        LOGGER.info(f"declared exchange: {frame.method.exchange}")

        self.basic_publish(body,
                           exchange=frame.method.exchange,
                           routing_key=routing_key,
                           publish_params=publish_params)

    def on_ready(self):
        """
        Connection hook, called when channel opened, meaning RMQConnection is
        ready for work.
        """
        LOGGER.info("producer connection ready")

        self._ready = True

    def on_close(self, permanent=False):
        """
        Connection hook, called when the channel or connection is closed.

        NOTE!
        This is NOT reported as an error because users are expected to
        configure their exchanges and queues accordingly to avoid losing data
        they cannot live without.

        :param permanent: bool
        """
        if permanent:
            LOGGER.critical("producer connection permanently closed")
        else:
            LOGGER.info("producer connection closed")

        self._ready = False

    def on_error(self):
        """
        Connection hook, called when the connection has encountered an error.
        """
        LOGGER.info("producer connection error")

        raise NotImplementedError
        # Possible errors:
        # * channel died and will not recover
        # * callback for operation failed
        # * declaration with faulty parameters attempted
        # TODO: Add possibility to signal user that an error has occurred.
