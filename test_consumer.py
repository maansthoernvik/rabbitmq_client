import time

from rmq_client.client import RMQClient


TEST_TOPIC_1 = "test1"
TEST_TOPIC_2 = "test2"


def sub_callback(message):
    print("test consumer got message: {}".format(message))


if __name__ == "__main__":
    client = RMQClient()

    client.start()

    client.subscribe(TEST_TOPIC_1, sub_callback)
    client.subscribe(TEST_TOPIC_2, sub_callback)

    try:
        while True:
            inp = input()

            if inp == "1":
                client.publish(TEST_TOPIC_1, "this is the message body")
            elif inp == "2":
                client.publish(TEST_TOPIC_2, "this is the message body")
    except KeyboardInterrupt:
        pass
