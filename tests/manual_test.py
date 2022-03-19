import time
import logging
import sys
import os

from enum import Enum, auto
from pika.exchange_type import ExchangeType
from typing import Union

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))

from rabbitmq_client import (  # noqa
    RMQConsumer,
    RMQProducer,
    ConsumeOK,
    QueueParams,
    ExchangeParams,
    ConsumeParams
)

LOGGER = logging.getLogger(__name__)


def main():
    # Logging
    logger = logging.getLogger("rabbitmq_client")
    logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)

    logger.addHandler(handler)

    # TODO: input argument to control verbostity?
    LOGGER.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)

    LOGGER.addHandler(handler)

    # Tester
    tester = Tester()

    # Start the tester, blocks
    tester.start()


class Tester:

    class Action(Enum):
        STOP = auto()
        CONSUME = auto()
        PRODUCE = auto()

        def __str__(self):
            return self.name

    @staticmethod
    def consume_callback(msg: Union[bytes, ConsumeOK], ack=None):
        """
        The tester specifies this general callback for consumes, add desired
        debug info here!
        """
        LOGGER.debug(f"got message: {msg}")

        if ack is not None:
            LOGGER.debug("manually acking")
            ack()

    @staticmethod
    def get_queue_info() -> Union[None, QueueParams]:
        """
        Asks the user for input about a queue to be declared.
        """
        queue_name = input("Queue name (or empty to skip): ")
        if not queue_name:
            LOGGER.info("No queue parameters")
            return None

        return QueueParams(queue_name)

    @staticmethod
    def get_exchange_info() -> Union[tuple[None, None],
                                     tuple[ExchangeParams, str]]:
        """
        Asks the user for input about an exchange to be declared.
        """
        exchange_name = input("Exchange name (or empty to skip): ")
        if not exchange_name:
            LOGGER.info("No exchange parameters")
            return None, None

        exchange_type = ExchangeType(
            input("Exchange type ('direct', 'fanout'): ")
        )
        routing_key = None
        if exchange_type is ExchangeType.direct:
            routing_key = input("Routing key: ")
        return (ExchangeParams(exchange_name, exchange_type=exchange_type),
                routing_key,)

    top_level_actions = [Action.STOP, Action.CONSUME, Action.PRODUCE]

    def __init__(self):
        self.keep_going = True

        # RMQ instances
        self.consumer = RMQConsumer()
        self.producer = RMQProducer()

    def start(self):
        self.consumer.start()
        self.producer.start()

        while True:
            time.sleep(0.1)
            if self.consumer.ready and self.producer.ready:
                break
            print("consume and producer are still not ready...")

        self.take_user_input()

    def take_user_input(self):
        while True:
            print("--- Select an action ---")
            for i, action in enumerate(Tester.top_level_actions):
                print(f"({i}): {action}")
            inp = input("Action: ")
            if inp == "":
                continue
            action = Tester.top_level_actions[int(inp)]

            if action is Tester.Action.STOP:
                self.stop()
                break

            elif action is Tester.Action.CONSUME:
                self.handle_consume_flow()

    def handle_consume_flow(self):
        print("- consume menu -")
        queue_params = Tester.get_queue_info()
        exchange_params, routing_key = Tester.get_exchange_info()
        consume_params = ConsumeParams(Tester.consume_callback)

        self.consumer.consume(consume_params,
                              queue_params=queue_params,
                              exchange_params=exchange_params,
                              routing_key=routing_key)

    def stop(self):
        self.consumer.stop()
        self.producer.stop()


if __name__ == "__main__":
    main()
