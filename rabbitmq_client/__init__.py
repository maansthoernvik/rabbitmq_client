import logging

from rabbitmq_client.defs import (  # noqa
    QueueParams,
    ExchangeParams,
    ConsumeParams
)


logging.getLogger(__name__).addHandler(logging.NullHandler())
