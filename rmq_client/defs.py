EXCHANGE_TYPE_FANOUT = "fanout"


class Subscription:
    """
    Class Subscription

    A Subscription work item.
    """

    topic: str

    def __init__(self, topic=""):
        """
        Initializes a subscription object.

        :param str topic: topic to subscribe to
        """
        self.topic = topic

    def __str__(self):
        return "{} {}".format(self.__class__, self.__dict__)


class Publish:
    """
    Class Publish

    A Publish work item.
    """

    topic: str
    routing_key: str
    message_content: str

    def __init__(self, message_content, topic="", routing_key=""):
        """
        Initializes a publish object.

        NOTE!

        Either topic, routing_key, or both must be set.

        :param str message_content: message to send
        :param str topic: topic to send the message on
        :param str routing_key: routing key to send the message on
        """
        assert topic or routing_key, "Either a topic or routing_key must be " \
                                     "given for a publish."

        self.topic = topic
        self.routing_key = routing_key
        self.message_content = message_content

    def __str__(self):
        return "{} {}".format(self.__class__, self.__dict__)


class Message:
    """
    Class Message

    A consumed message.
    """

    topic: str
    message_content: bytes

    def __init__(self, topic, message_content):
        """
        Initializes a message object.

        :param str topic: topic the message was posted on
        :param bytes message_content: message content in bytes format
        """
        self.topic = topic
        self.message_content = message_content

    def __str__(self):
        return "{} {}".format(self.__class__, self.__dict__)


class ConnectionStopped:
    """
    Class ConnectionStopped

    A message sent internally when an RMQ connection has been stopped.
    """
    pass
