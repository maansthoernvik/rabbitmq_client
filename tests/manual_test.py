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
from rabbitmq_client.consumer import _gen_consume_key  # noqa

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
        # Top
        STOP = auto()
        CONSUME = auto()
        PRODUCE = auto()

        # Queue/Exchange
        NO_QUEUE = auto()
        AUTO_QUEUE = auto()
        NEW_QUEUE = auto()
        EXISTING_QUEUE = auto()

        NO_EXCHANGE = auto()
        NEW_EXCHANGE = auto()
        EXISTING_EXCHANGE = auto()

        def __str__(self):
            return self.name

    top_level_actions = [Action.STOP, Action.CONSUME, Action.PRODUCE]

    YES = "y"
    NO = "n"  # unused

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
    def make_publish_message(queue_params: QueueParams = None,
                             exchange_params: ExchangeParams = None,
                             routing_key: str = "") -> str:
        return _gen_consume_key(
            queue=queue_params.queue if queue_params else "",
            exchange=exchange_params.exchange if exchange_params else "",
            routing_key=routing_key)

    @staticmethod
    def get_queue_info() -> QueueParams:
        """
        Asks the user for input about a queue to be declared.
        """
        queue_name = input("Queue name (or empty to skip): ")

        durable = input("Durable (y/n): ")
        durable = True if durable == Tester.YES else False

        exclusive = input("Exclusive (y/n): ")
        exclusive = True if exclusive == Tester.YES else False

        auto_delete = input("Auto delete (y/n): ")
        auto_delete = True if auto_delete == Tester.YES else False

        return QueueParams(queue_name,
                           durable=durable,
                           exclusive=exclusive,
                           auto_delete=auto_delete)

    @staticmethod
    def get_exchange_info() -> Union[tuple[None, str],
                                     tuple[ExchangeParams, str]]:
        """
        Asks the user for input about an exchange to be declared.
        """
        exchange_name = input("Exchange name (or empty to skip): ")
        if not exchange_name:
            LOGGER.info("No exchange parameters")
            return None, ""

        while True:
            try:
                exchange_type = ExchangeType(
                    input("Exchange type ('direct', 'fanout'): ")
                )
                break
            except ValueError as e:
                print(e)

        routing_key = ""
        if exchange_type is ExchangeType.direct:
            routing_key = input("Routing key: ")

        durable = input("Durable (y/n): ")
        durable = True if durable == Tester.YES else False

        auto_delete = input("Auto delete (y/n): ")
        auto_delete = True if auto_delete == Tester.YES else False

        internal = input("Internal (y/n): ")
        internal = True if internal == Tester.YES else False

        return (ExchangeParams(exchange_name,
                               exchange_type=exchange_type,
                               durable=durable,
                               auto_delete=auto_delete,
                               internal=internal),
                routing_key,)

    def __init__(self):
        self.keep_going = True

        # RMQ instances
        self.consumer = RMQConsumer()
        self.producer = RMQProducer()

        # Caches
        self.queues: list[QueueParams] = list()
        self.exchanges: list[ExchangeParams] = list()

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

            elif action is Tester.Action.PRODUCE:
                self.handle_produce_flow()

    def handle_consume_flow(self):
        print("- consume menu -")
        queue_opts = [Tester.Action.AUTO_QUEUE,
                      Tester.Action.NEW_QUEUE,
                      Tester.Action.EXISTING_QUEUE]
        for i, action in enumerate(queue_opts):
            print(f"({i}): {action}")
        inp = input("Action: ")
        action = queue_opts[int(inp)]

        queue_params = None
        if action is Tester.Action.AUTO_QUEUE:
            pass

        elif action is Tester.Action.NEW_QUEUE:
            queue_params = Tester.get_queue_info()

        elif action is Tester.Action.EXISTING_QUEUE:
            queue_params = self.select_existing_queue()

        exchange_opts = [Tester.Action.NO_EXCHANGE,
                         Tester.Action.NEW_EXCHANGE,
                         Tester.Action.EXISTING_EXCHANGE]
        for i, action in enumerate(exchange_opts):
            print(f"({i}): {action}")
        inp = input("Action: ")
        action = exchange_opts[int(inp)]

        exchange_params = None
        routing_key = ""
        if action is Tester.Action.NO_EXCHANGE:
            pass

        elif action is Tester.Action.NEW_EXCHANGE:
            exchange_params, routing_key = Tester.get_exchange_info()

        elif action is Tester.Action.EXISTING_EXCHANGE:
            exchange_params = self.select_existing_exchange()
            if exchange_params.exchange_type is ExchangeType.direct:
                routing_key = input("Routing key: ")

        consume_params = ConsumeParams(Tester.consume_callback)

        try:
            self.consumer.consume(consume_params,
                                  queue_params=queue_params,
                                  exchange_params=exchange_params,
                                  routing_key=routing_key)
        except ValueError as e:
            print(e)

        # store declarations
        self.store_declarations(queue_params=queue_params,
                                exchange_params=exchange_params)

    def handle_produce_flow(self):
        print("- produce menu -")
        queue_opts = [Tester.Action.NO_QUEUE,
                      Tester.Action.NEW_QUEUE,
                      Tester.Action.EXISTING_QUEUE]
        for i, action in enumerate(queue_opts):
            print(f"({i}): {action}")
        inp = input("Action: ")
        action = queue_opts[int(inp)]

        queue_params = None
        if action is Tester.Action.NEW_QUEUE:
            queue_params = Tester.get_queue_info()

        elif action is Tester.Action.EXISTING_QUEUE:
            queue_params = self.select_existing_queue()

        exchange_opts = [Tester.Action.NO_EXCHANGE,
                         Tester.Action.NEW_EXCHANGE,
                         Tester.Action.EXISTING_EXCHANGE]
        for i, action in enumerate(exchange_opts):
            print(f"({i}): {action}")
        inp = input("Action: ")
        action = exchange_opts[int(inp)]

        exchange_params = None
        routing_key = ""
        if action is Tester.Action.NO_EXCHANGE:
            pass

        elif action is Tester.Action.NEW_EXCHANGE:
            exchange_params, routing_key = Tester.get_exchange_info()

        elif action is Tester.Action.EXISTING_EXCHANGE:
            exchange_params = self.select_existing_exchange()
            if exchange_params.exchange_type is ExchangeType.direct:
                routing_key = input("Routing key: ")

        msg = Tester.make_publish_message(queue_params,
                                          exchange_params,
                                          routing_key)

        try:
            self.producer.publish(f"{msg}".encode(),
                                  exchange_params=exchange_params,
                                  routing_key=routing_key,
                                  queue_params=queue_params)
        except ValueError as e:
            print(e)

        # store declarations
        self.store_declarations(queue_params=queue_params,
                                exchange_params=exchange_params)

    def store_declarations(self,
                           queue_params=None,
                           exchange_params=None):
        """
        The declaration store is never cleared since it only stores parameters
        of what 'could' be declared, and has nothing to do with neither
        consumer nor producer caches.
        """
        if queue_params:
            if queue_params not in self.queues:
                self.queues.append(queue_params)
        if exchange_params:
            if exchange_params not in self.exchanges:
                self.exchanges.append(exchange_params)

    def select_existing_queue(self) -> QueueParams:
        """
        Queue declarations are aggregated for the consumer and producer to
        avoid re-declaration mistakes and simplify testing.
        """
        for i, queue_param in enumerate(self.queues):
            print(f"({i}): {queue_param.queue}")
        inp = input("Queue: ")
        return self.queues[int(inp)]

    def select_existing_exchange(self) -> ExchangeParams:
        """
        Exchange declarations are aggregated for the consumer and producer to
        avoid re-declaration mistakes and simplify testing.
        """
        for i, exchange_param in enumerate(self.exchanges):
            print(f"({i}): {exchange_param.exchange}")
        inp = input("Exchange: ")
        return self.exchanges[int(inp)]

    def stop(self):
        self.consumer.stop()
        self.producer.stop()


if __name__ == "__main__":
    main()
