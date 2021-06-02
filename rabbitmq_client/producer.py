import functools
import logging

from rabbitmq_client.defs import QueueParams, QueueBindParams, ConsumeOK
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

    def on_queue_declared(self, frame):
        """
        """
        LOGGER.info(f"declared queue: {frame.method.queue}")

    def on_exchange_declared(self, frame):
        """
        """
        LOGGER.info(f"declared exchange: {frame.method.exchange}")

    def on_queue_bound(self, frame):
        """
        """
        pass

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

        # Possible errors:
        # * channel died and will not recover
        # * callback for operation failed
        # * declaration with faulty parameters attempted
        # TODO: Add possibility to signal user that an error has occurred.
