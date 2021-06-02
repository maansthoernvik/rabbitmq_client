import logging

from rabbitmq_client.connection import RMQConnection  # noqa
from rabbitmq_client.consumer import RMQConsumer  # noqa
from rabbitmq_client.defs import (  # noqa
    QueueParams,
    ExchangeParams,
    ConsumeParams,
    QueueBindParams,
    ConsumeOK,
)


logging.getLogger(__name__).addHandler(logging.NullHandler())
