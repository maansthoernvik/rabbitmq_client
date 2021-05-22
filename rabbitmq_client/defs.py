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
        """
        self.on_message_callback = on_message_callback
        self.queue = queue
        self.auto_ack = auto_ack
        self.exclusive = exclusive
        self.consumer_tag = consumer_tag
        self.arguments = arguments
