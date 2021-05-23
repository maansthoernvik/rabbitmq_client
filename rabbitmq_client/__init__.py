import logging

from rabbitmq_client.defs import (  # noqa
    QueueParams,
    ExchangeParams,
    ConsumeParams
)
from rabbitmq_client.new_consumer import RMQConsumer  # noqa


logging.getLogger(__name__).addHandler(logging.NullHandler())
