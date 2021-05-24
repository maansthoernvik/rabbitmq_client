import logging

from rabbitmq_client.new_connection import RMQConnection  # noqa
from rabbitmq_client.new_consumer import RMQConsumer  # noqa
from rabbitmq_client.defs import (  # noqa
    QueueParams,
    ExchangeParams,
    ConsumeParams,
    QueueBindParams,
    ConsumeOK,
)


logging.getLogger(__name__).addHandler(logging.NullHandler())
