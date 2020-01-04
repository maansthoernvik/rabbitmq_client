EXCHANGE_TYPE_FANOUT = "fanout"


class Subscription:

    topic: str

    def __init__(self, topic=""):
        self.topic = topic


class Message:

    topic: str
    message_content: str

    def __init__(self, topic, message_content):
        self.topic = topic
        self.message_content = message_content
