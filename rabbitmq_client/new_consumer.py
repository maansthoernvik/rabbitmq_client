import logging

from rabbitmq_client.new_connection import RMQConnection


LOGGER = logging.getLogger(__name__)


class RMQConsumer(RMQConnection):
    """
    Generic consumer implementation using the RMQConnection base class to ease
    connection and channel handling.

    The 'consume' method provided by this consumer implementation accepts
    parameters as they will be passed to the pika library to avoid going out
    of its way to create another layer of special handling. If you need
    information on what pika accepts as parameters for queue
    declarations/bindings and exchange declarations, see pika's official
    documentation here: https://pika.readthedocs.io/en/stable/intro.html,
    specifically its 'Core Class and Module Documentation'.
    """

    def __init__(self, connection_parameters=None):
        """
        :param connection_parameters: pika.ConnectionParameters
        """
        super().__init__(connection_parameters=connection_parameters)
        self.ready = False

    def start(self):
        """Starts the consumer by initiating the underlying connection."""
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

    def on_ready(self):
        """
        Connection hook, called when channel opened, meaning RMQConnection is
        ready for work.
        """
        LOGGER.info("consumer connection ready")

        self.ready = True

    def on_close(self):
        """Connection hook, called when the channel or connection is closed."""
        LOGGER.info("consumer connection closed")

        self.ready = False

    def on_error(self):
        """
        Connection hook, called when the connection has encountered an error.
        """
        LOGGER.info("consumer connection error")

    def consume(self, queue, exchange, routing_key, consume):
        """
        General consumer interface, when wanting to consumer messages sent to a
        specific queue or exchange. Input parameters are passed directly to
        pika for simplicity's sake, to avoid re-inventing the wheel with
        special sanity checks and abstractions. Errors in declarations passed
        here can be discovered by users through the 'on_error' hook, which is
        called when a declaration or binding has failed due to the parameters
        passed here.

        RMQConsumer will automatically re-issue queue and exchange declarations
        and bindings if the connection is for some reason closed at any point.
        RMQConsumer notices this through the underlying RMQConnection calling
        'on_ready' after first calling 'on_close'.

        This method may be called before the underlying RMQConnection has
        successfully established a connection and channel to RabbitMQ. Those
        buffered actions called before RMQConnection reports it is ready,
        through the 'on_ready' hook, will be issued immediately upon 'on_ready'
        being called. Monitor the RMQConsumer 'ready' property to keep track of
        when the RMQConnection has reported it is ready.

        # noqa: E501
        :param queue: queue declaration parameters: https://pika.readthedocs.io/en/stable/modules/channel.html#pika.channel.Channel.queue_declare
        :param exchange: exchange declaration parameters: https://pika.readthedocs.io/en/stable/modules/channel.html#pika.channel.Channel.exchange_declare
        :param routing_key: for binding
        :param consume: consume parameters: https://pika.readthedocs.io/en/stable/modules/channel.html#pika.channel.Channel.exchange_declare
        """
        raise NotImplementedError()
