from .common_defs import Printable


class Subscription(Printable):
    """
    A subscription work item.
    """
    topic: str
    queue_name: str

    def __init__(self, topic):
        """
        :param str topic: topic to subscribe to
        """
        self.topic = topic

    def set_queue_name(self, queue_name):
        """
        Setter for queue_name.

        :param queue_name: new queue_name
        """
        self.queue_name = queue_name
        return self.queue_name


class RPCServer(Printable):
    """
    An RPCServer work item.
    """
    queue_name: str

    def __init__(self, queue_name):
        """
        :param queue_name: name of request queue to declare
        """
        self.queue_name = queue_name


class RPCClient(Printable):
    """
    An RPCClient work item.
    """
    queue_name: str

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
