import time

from rmq_client.client import RMQClient


if __name__ == "__main__":
    client = RMQClient()

    client.start()

    try:
        while True:
            input()
    except KeyboardInterrupt:
        pass
