EXCHANGE_TYPE_FANOUT = "fanout"


class Subscription:

    topic: str

    def __init__(self, topic=""):
        self.topic = topic

    def __str__(self):
        return "{} {}".format(self.__class__, self.__dict__)


class Publish:

    topic: str
    routing_key: str
    message_content: str

    def __init__(self, message_content, topic="", routing_key=""):
        assert topic or routing_key, "Either a topic or routing_key must be " \
                                     "given for a publish."

        self.topic = topic
        self.routing_key = routing_key
        self.message_content = message_content

    def __str__(self):
        return "{} {}".format(self.__class__, self.__dict__)


class Message:

    topic: str
    message_content: str

    def __init__(self, topic, message_content):
        self.topic = topic
        self.message_content = message_content

    def __str__(self):
        return "{} {}".format(self.__class__, self.__dict__)
