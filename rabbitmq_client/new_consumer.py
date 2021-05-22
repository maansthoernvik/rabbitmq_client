import logging

from rabbitmq_client.new_connection import RMQConnection


LOGGER = logging.getLogger(__name__)


def _gen_consume_key(queue_params=None,
                     exchange_params=None,
                     routing_key=None):
    """
    :param queue_params: rabbitmq_client.QueueParams
    :param exchange_params: rabbitmq_client.ExchangeParams
    :param routing_key: str
    :return: str
    """
    key_list = []
    key_list.append(queue_params.queue) if queue_params is not None else ""
    (
        key_list.append(exchange_params.exchange)
        if exchange_params is not None else ""
    )
    key_list.append(routing_key) if routing_key is not None else ""

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

        self.ready = False
        self._consumes = dict()

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

        for _key, consume in self._consumes.items():
            if consume.exchange_params is None:
                self.declare_queue(
                    consume.queue_params,
                    consume_params=consume.consume_params
                )
            else:
                self.declare_exchange(
                    consume.exchange_params,
                    queue_params=consume.queue_params,
                    routing_key=consume.routing_key,
                    consume_params=consume.consume_params
                )

    def on_close(self):
        """Connection hook, called when the channel or connection is closed."""
        LOGGER.info("consumer connection closed")

        self.ready = False

        for _key, consume in self._consumes.items():
            consume.consumer_tag = None

    def on_error(self):
        """
        Connection hook, called when the connection has encountered an error.
        """
        LOGGER.info("consumer connection error")

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
        # 1. Checks
        if queue_params is None and exchange_params is None:
            raise ValueError(
                "You need to provide either a queue, and exchange, or both, "
                "else there is nothing to consume from..."
            )

        consume_key = _gen_consume_key(
            queue_params, exchange_params, routing_key
        )
        if self._consumes.get(consume_key) is not None:
            raise ValueError(
                "That combination of queue + exchange + routing key already "
                "exists."
            )

        # 2. Update consumer instance
        self._consumes[consume_key] = RMQConsume(
            consume_params, queue_params, exchange_params, routing_key
        )

        # 3. Start declaring shit
        if self.ready:
            if exchange_params is None:
                self.declare_queue(
                    queue_params,
                    consume_params=consume_params
                )
            else:
                self.declare_exchange(
                    exchange_params,
                    queue_params=queue_params,
                    routing_key=routing_key,
                    consume_params=consume_params
                )

        return consume_key
