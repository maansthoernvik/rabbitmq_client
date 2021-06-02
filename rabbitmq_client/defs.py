from pika.exchange_type import ExchangeType


class QueueParams:

    def __init__(self,
                 queue,
                 durable=False,
                 exclusive=False,
                 auto_delete=False,
                 arguments=None):
        """
        # noqa 501
        https://pika.readthedocs.io/en/stable/modules/channel.html#pika.channel.Channel.queue_declare

        Excluding the "passive" and "callback" options. Passive may not be used
        in this context, and callback is used by the connection object.
        """
        self.queue = queue
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
        self.arguments = arguments


class ExchangeParams:

    def __init__(self,
                 exchange,
                 exchange_type=ExchangeType.direct,
                 durable=False,
                 auto_delete=False,
                 internal=False,
                 arguments=None):
        """
        # noqa 501
        https://pika.readthedocs.io/en/stable/modules/channel.html#pika.channel.Channel.exchange_declare

        Excluding the "passive" and "callback" options. Passive may not be used
        in this context, and callback is used by the connection object.
        """
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.durable = durable
        self.auto_delete = auto_delete
        self.internal = internal
        self.arguments = arguments


class ConsumeParams:

    def __init__(self,
                 on_message_callback,
                 queue=None,
                 auto_ack=False,
                 exclusive=False,
                 consumer_tag=None,
                 arguments=None):
        """
        # noqa 501
        https://pika.readthedocs.io/en/stable/modules/channel.html#pika.channel.Channel.basic_consume

        Excluding the "passive" and "callback" options. Passive may not be used
        in this context, and callback is used by the connection object.

        Queue is optional and must not be provided in case a queue is to be
        declared before consuming will start, for example through a call to
        RMQConsumer.consume, which takes as input QueueParams.
        """
        self.on_message_callback = on_message_callback
        self._queue = queue
        self.auto_ack = auto_ack
        self.exclusive = exclusive
        self._consumer_tag = consumer_tag
        self.arguments = arguments

    @property
    def queue(self):
        return self._queue

    @queue.setter
    def queue(self, new_value):
        self._queue = new_value

    @property
    def consumer_tag(self):
        return self._consumer_tag

    @consumer_tag.setter
    def consumer_tag(self, new_value):
        self._consumer_tag = new_value


class QueueBindParams:

    def __init__(self,
                 queue,
                 exchange,
                 routing_key=None,
                 arguments=None):
        """
        # noqa 501
        https://pika.readthedocs.io/en/stable/modules/channel.html#pika.channel.Channel.queue_bind
        """
        self.queue = queue
        self.exchange = exchange
        self.routing_key = routing_key
        self.arguments = arguments


class ConsumeOK:

    def __init__(self, consumer_tag):
        """
        :param consumer_tag: str
        """
        self.consumer_tag = consumer_tag
