from .common_defs import Printable


class Consumer(Printable):
    """
    Base class for consumers
    """
    queue_name: str

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
    A subscription work item.
    """
    topic: str

    def __init__(self, topic):
        """
        :param str topic: topic to subscribe to
        """
        self.topic = topic


class RPCServer(Consumer):
    """
    An RPCServer work item.
    """

    def __init__(self, queue_name):
        """
        :param queue_name: name of request queue to declare
        """
        self.queue_name = queue_name


class RPCClient(Consumer):
    """
    An RPCClient work item.
    """

    def __init__(self, queue_name):
        """
        :param queue_name: name of response queue to declare
        """
        self.queue_name = queue_name


class ConsumedMessage(Printable):
    """
    A consumed message.
    """
    # General
    message: bytes

    # Routing
    exchange = ""
    routing_key = ""

    # RPC
    correlation_id: str = None
    reply_to: str = None

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

        if exchange:
            self.exchange = exchange

        if routing_key:
            self.routing_key = routing_key

        if correlation_id:
            self.correlation_id = correlation_id

        if reply_to:
            self.reply_to = reply_to


class ConsumeOk(Printable):
    """
    Confirmation of a completed consume
    """
    consumer_tag: str
    consume = None  # A consume work item

    def __init__(self, consumer_tag, consume):
        """
        :param consumer_tag: consumer tag used for cancellation
        :param consume: consume issued
        """
        self.consumer_tag = consumer_tag
        self.consume = consume
