from .common_defs import Printable


class Subscription(Printable):
    """
    Class Subscription

    A Subscription work item.
    """
    topic: str
    queue_name: str

    def __init__(self, topic):
        """
        Initializes a subscription object.

        :param str topic: topic to subscribe to
        """
        self.topic = topic

    def set_queue_name(self, queue_name):
        self.queue_name = queue_name
        return self.queue_name


class RPCServer(Printable):
    queue_name: str

    def __init__(self, queue_name):
        self.queue_name = queue_name


class RPCClient(Printable):
    queue_name: str

    def __init__(self, queue_name):
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
        Initializes a message object.
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
