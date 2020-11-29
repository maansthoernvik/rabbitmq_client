from rabbitmq_client.common_defs import Printable


"""
Queue Properties
"""


AUTO_GEN_QUEUE_NAME = ""


"""
Consume work item types
"""


class Consumer(Printable):
    """
    Base class for consumers, all consumes have a queue name. This is not set
    until queue name is known in case of fanout subscriptions (Subscription
    work item types).
    """

    def set_queue_name(self, queue_name):
        """
        Setter for queue_name

        :param queue_name: new queue_name
        :return: new consumer_tag
        """
        self.queue_name = queue_name
        return self.queue_name


class Subscription(Consumer):
    """
    A Subscription work item.

    Queue name will be set after queue declaration. Subscriptions uses the
    fanout exchange type, and a queue is generated with a random name to
    receive exchange-bound messages on. This consume work item will receive a
    set queue name after its queue has been declared, so it is possible to find
    out the queue name when receiving the ConsumeOk from the consumer
    connection.
    """

    def __init__(self, topic):
        """
        :param str topic: topic to subscribe to
        """
        self.topic = topic


class RPCServer(Consumer):
    """
    An RPC Server work item.
    """

    def __init__(self, queue_name):
        """
        :param queue_name: name of request queue to declare
        """
        self.queue_name = queue_name


class RPCClient(Consumer):
    """
    An RPC Client work item.
    """

    def __init__(self, queue_name):
        """
        :param queue_name: name of response queue to declare
        """
        self.queue_name = queue_name


class CommandQueue(Consumer):
    """
    A Command Queue work item.
    """

    def __init__(self, queue_name):
        """
        :param queue_name: name for the command queue
        :type queue_name: str
        """
        self.queue_name = queue_name


"""
Monitoring thread interpretable messages, mostly feedback from the Consumer
connection.
"""


class ConsumeOk(Printable):
    """
    Confirmation of a completed consume
    """

    def __init__(self, consumer_tag, consume):
        """
        :param consumer_tag: consumer tag used for cancellation
        :param consume: consume issued
        """
        self.consumer_tag = consumer_tag
        self.consume = consume


class ConsumedMessage(Printable):
    """
    A consumed message.
    """

    def __init__(self,
                 message,
                 exchange,
                 routing_key,
                 correlation_id,
                 reply_to):
        """
        :param message: message received
        :param exchange: exchange received on
        :param routing_key: routing key received on
        :param correlation_id: ID for RPC request/response
        :param reply_to: reply queue for RPC request
        """
        self.message = message
        self.exchange = exchange if exchange else ""
        self.routing_key = routing_key if routing_key else ""
        self.correlation_id = correlation_id if correlation_id else None
        self.reply_to = reply_to if reply_to else None


class StopConsumer(Printable):
    """
    Tell the monitoring thread to stop, posted not from the consumer
    connection, but a Consumer class instance, when stop is called.
    """
    pass
