import time

from rmq_client.client import RMQClient


def sub_callback(message):
    print("test consumer got message: {}".format(message))


if __name__ == "__main__":
    client = RMQClient()

    client.start()

    client.subscribe("test", sub_callback)

    try:
        while True:
            input()
    except KeyboardInterrupt:
        pass
