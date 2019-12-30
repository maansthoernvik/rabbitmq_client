import signal
import time

from context import rmq_client

from rmq_client.rmq_client import RMQClient


client = None


def interrupt(signum, frame):
    print("test_subscriber: SIGINT")
    client.stop()


def callback(channel, method, properties, body):
    print(body)
    print(channel)
    print(method)
    print(properties)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, interrupt)

    client = RMQClient()
    print("Created Client")
    client.start()
    print("Started Client")

    print("Creating consumer")
    consumer = client.create_consumer()
    print("Created consumer")
    #consumer.subscribe("tests", "tests.tests", callback)

    while True:
        time.sleep(1)
