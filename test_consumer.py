import time

from rmq_client.client import RMQClient


TEST_TOPIC = "test"


def sub_callback(message):
    print("test consumer got message: {}".format(message))


if __name__ == "__main__":
    client = RMQClient()

    client.start()

    client.subscribe(TEST_TOPIC, sub_callback)

    try:
        while True:
            inp = input()

            if inp:
                client.publish(TEST_TOPIC, "this is the message body")
    except KeyboardInterrupt:
        pass
