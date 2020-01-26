from .common_defs import Printable


class GenProduce(Printable):
    """
    Base for producing work classes.
    """
    MAX_ATTEMPTS = 3

    attempts: int

    def __init__(self):
        """"""
        self.attempts = 0

    def attempt(self):
        """
        Increments the attempt counter.
        """
        self.attempts += 1

        return self


class Publish(GenProduce):
    """
    A Publish work item.
    """
    topic: str
    message: bytes

    def __init__(self, topic, message):
        """
        :param topic: publishing topic
        :param bytes message: message to publish
        """
        self.topic = topic
        self.message = message
        super().__init__()


class RPCRequest(GenProduce):
    """
    An RPC request work item.
    """
    receiver: str
    message: bytes
    correlation_id: str
    reply_to: str

    def __init__(self, receiver, message, correlation_id, reply_to):
        """
        :param receiver: receiver of the RPC request
        :param message: content
        :param correlation_id: ID of request
        :param reply_to: reply queue for answer
        """
        self.receiver = receiver
        self.message = message
        self.correlation_id = correlation_id
        self.reply_to = reply_to
        super().__init__()


class RPCResponse(GenProduce):
    """
    An RPC response work item.
    """
    receiver: str
    message: bytes
    correlation_id: str

    def __init__(self, receiver, message, correlation_id):
        """
        :param receiver: sender of an RPC request
        :param message: content
        :param correlation_id: ID of request that response is for
        """
        self.receiver = receiver
        self.message = message
        self.correlation_id = correlation_id
        super().__init__()