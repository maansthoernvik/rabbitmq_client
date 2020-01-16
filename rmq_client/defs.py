EXCHANGE_TYPE_FANOUT = "fanout"


class Subscription:
    """
    Class Subscription

    A Subscription work item.
    """
    TOPIC = 0
    RPC_REPLY = 1
    RPC_REQUEST = 2

    topic: str
    sub_type: int

    kwargs: dict

    def __init__(self, topic="", sub_type=TOPIC):
        """
        Initializes a subscription object.

        :param str topic: topic to subscribe to
        """
        self.topic = topic
        self.sub_type = sub_type

    def __str__(self):
        return "{} {}".format(self.__class__, self.__dict__)


class Publish:
    """
    Class Publish

    A Publish work item.
    """

    exchange: str = ""
    routing_key: str = ""
    message_content: str
    correlation_id: str
    reply_to: str

    attempts: int

    MAX_ATTEMPTS = 3

    def __init__(self,
                 message_content,
                 topic,
                 correlation_id=None,
                 reply_to=None):
        """
        Initializes a publish object.

        NOTE!

        Either topic, routing_key, or both must be set.

        :param str message_content: message to send
        :param str topic: topic to send the message on
        :param str routing_key: routing key to send the message on
        """

        self.correlation_id = correlation_id
        self.reply_to = reply_to

        if reply_to or correlation_id:
            self.routing_key = topic
        else:
            self.exchange = topic

        self.message_content = message_content

        self.attempts = 0

    def attempt(self):
        self.attempts += 1

        return self

    def __str__(self):
        return "{} {}".format(self.__class__, self.__dict__)


class ConsumedMessage:
    """
    Class Message

    A consumed message.
    """

    topic: str
    message_content: str
    correlation_id: str
    reply_to: str

    def __init__(self,
                 topic,
                 message_content,
                 correlation_id=None,
                 reply_to=None):
        """
        Initializes a message object.

        :param str topic: topic the message was posted on
        :param str message_content: message content in bytes format
        """
        self.topic = topic
        self.message_content = message_content
        self.correlation_id = correlation_id
        self.reply_to = reply_to

    def __str__(self):
        return "{} {}".format(self.__class__, self.__dict__)


class RPCReply:

    message_content: str
    correlation_id: str

    def __init__(self, message_content, correlation_id):
        self.message_content = message_content
        self.correlation_id = correlation_id

    def __str__(self):
        return "{} {}".format(self.__class__, self.__dict__)
