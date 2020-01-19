from .common_defs import Printable


class GenProduce(Printable):
    MAX_ATTEMPTS = 3

    attempts: int

    def __init__(self):
        self.attempts = 0

    def attempt(self):
        self.attempts += 1

        return self


class Publish(GenProduce):
    """
    Class Publish

    A Publish work item.
    """
    topic: str
    message: bytes

    def __init__(self, topic, message):
        """
        Initializes a publish object.
        """
        self.topic = topic
        self.message = message
        super().__init__()


class RPCRequest(GenProduce):
    receiver: str
    message: bytes
    correlation_id: str
    reply_to: str

    def __init__(self, receiver, message, correlation_id, reply_to):
        self.receiver = receiver
        self.message = message
        self.correlation_id = correlation_id
        self.reply_to = reply_to
        super().__init__()


class RPCResponse(GenProduce):
    receiver: str
    message: bytes
    correlation_id: str

    def __init__(self, receiver, message, correlation_id):
        self.receiver = receiver
        self.message = message
        self.correlation_id = correlation_id
        super().__init__()