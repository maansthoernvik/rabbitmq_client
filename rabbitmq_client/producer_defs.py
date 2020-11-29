from rabbitmq_client.common_defs import Printable


"""
Exchange properties
"""


DEFAULT_EXCHANGE = ""


"""
Produce work item types
"""


class GenProduce(Printable):
    """
    Base for producing work classes.
    """
    MAX_ATTEMPTS = 3

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


class Command(GenProduce):
    """
    A Command work item.
    """

    def __init__(self, command_queue, command):
        """
        :param command_queue: name of the command queue to send the command to
        :type command_queue: str
        :param command: command to send to command queue
        :type command: bytes
        """
        self.command_queue = command_queue
        self.command = command
        super().__init__()
