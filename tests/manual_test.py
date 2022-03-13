import signal
import logging
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))

from rabbitmq_client import RMQConsumer, RMQProducer


def main():
    # Logging
    logger = logging.getLogger("rabbitmq_client")
    logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)

    logger.addHandler(handler)

    # Tester
    tester = Tester()

    # Start the tester, blocks
    tester.start()


class Tester:

    STOP = "stop"
    CONSUME = "consume"
    PRODUCE = "produce"

    top_level_actions = [CONSUME, PRODUCE, STOP]

    def __init__(self):
        self.keep_going = True

        # RMQ instances
        self.consumer = RMQConsumer()
        self.producer = RMQProducer()

    def start(self):
        self.consumer.start()
        self.producer.start()

        self.take_user_input()

    def take_user_input(self):
        while self.keep_going:
            print("--- Select an action ---")
            for i, action in enumerate(Tester.top_level_actions):
                print(f"({i}): {action}")
            inp = input("Action: ")

            self.handle_top_level_input(Tester.top_level_actions[int(inp)])

    def handle_top_level_input(self, inp):
        if inp == Tester.STOP:
            print("stopping tester...")
            self.stop()
            self.keep_going = False

        elif inp == Tester.CONSUME:
            self.handle_consume()

        elif inp == Tester.PRODUCE:
            self.handle_produce()

    def handle_consume(self):
        pass

    def handle_produce(self):
        pass

    def stop(self):
        self.consumer.stop()
        self.producer.stop()


if __name__ == "__main__":
    main()
