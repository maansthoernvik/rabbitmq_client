import logging

from rabbitmq_client.new_connection import RMQConnection


LOGGER = logging.getLogger(__name__)


def _gen_consume_key(queue=None,
                     exchange=None,
                     routing_key=None):
    """
    :param queue: rabbitmq_client.QueueParams
    :param exchange: rabbitmq_client.ExchangeParams
    :param routing_key: str
    :return: str
    """
    key_list = []
    key_list.append(queue.queue) if queue is not None else ""
    key_list.append(exchange.exchange) if exchange is not None else ""
    key_list.append(routing_key) if routing_key is not None else ""

    separator = "|"

    return separator.join(key_list)


class ConsumeInstance:
    """
    Used by the RMQConsumer to keep track of which consumes have been started
    so far, and to be able to re-distribute consumes should the underlying
    connection experience a hiccup.
    """

    def __init__(self,
                 consume,
                 queue,
                 exchange,
                 routing_key):
        """
        :param consume: rabbitmq_client.ConsumeParams
        :param queue: rabbitmq_client.QueueParams
        :param exchange: rabbitmq_client.ExchangeParams
        :param routing_key: str
        """
        self.consume = consume
        self.queue = queue
        self.exchange = exchange
        self.routing_key = routing_key


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
        self.ready = False
        self._running_consumes = dict()

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

    def consume(self,
                consume,
                queue=None,
                exchange=None,
                routing_key=None):
        """
        General consumer interface, when wanting to consumer messages sent to a
        specific queue or exchange. Input parameters are a subset of those used
        by pika, to avoid re-inventing the wheel with special abstractions.
        The only parameters that have been removed are the "passive" and
        "callback" options.

        Some restrictions apply:
        1. EITHER queue OR exchange OR both MUST be set.
        2. Calling consume a second time with the same queue name + exchange
           name + routing key will result in an exception.

        RMQConsumer will automatically re-issue queue and exchange declarations
        and bindings if the connection is for some reason closed and re-opened
        at any point.

        This method may be called before the underlying RMQConnection has
        successfully established a connection and channel to RabbitMQ. Those
        buffered actions called before RMQConnection reports it is ready,
        through the 'on_ready' hook, will be issued immediately upon 'on_ready'
        being called. Monitor the RMQConsumer 'ready' property to keep track of
        when the RMQConnection has reported it is ready.

        :param consume: rabbitmq_client.ConsumeParams
        :param queue: rabbitmq_client.QueueParams
        :param exchange: rabbitmq_client.ExchangeParams
        :param routing_key: str
        """
        if queue is None and exchange is None:
            raise ValueError(
                "You need to provide either a queue or exchange or both, "
                "else there is nothing to consume from."
            )

        consume_key = _gen_consume_key(queue, exchange, routing_key)
        if self._running_consumes.get(consume_key) is not None:
            raise ValueError(
                "That combination of queue + exchange + routing key has "
                "already got a consume running."
            )

        self._running_consumes[consume_key] = ConsumeInstance(
            consume, queue, exchange, routing_key
        )
