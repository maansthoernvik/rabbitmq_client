import logging

from rmq_client.client import RMQClient


TEST_TOPIC_1 = "test1"
TEST_TOPIC_2 = "test2"


def sub_callback(message):
    print("test consumer got message: {}".format(message))


def rpc_request_callback(message):
    print("got RPC request: {}".format(message))

    response = "default"

    if message == "test":
        response = "42"
    elif message == "testing":
        response = "84"

    return response


if __name__ == "__main__":
    print("instantiating RMQ client")
    client = RMQClient(log_level=logging.DEBUG)
    print("starting RMQ client")
    client.start()

    client.subscribe(TEST_TOPIC_1, sub_callback)
    client.subscribe(TEST_TOPIC_2, sub_callback)

    client.enable_rpc_server("rpc_server", rpc_request_callback)
    client.enable_rpc_client()

    try:
        while True:
            inp = input()

            if inp == "1":
                client.publish(TEST_TOPIC_1, "this is the message body")

            elif inp == "2":
                client.publish(TEST_TOPIC_2, "this is the message body")

            elif inp == "3":
                reply = client.rpc_call("rpc_server", "testing RPC call")
                print("Got RPC reply: {}".format(reply))
                reply = client.rpc_call("rpc_server", "test")
                print("Got RPC reply: {}".format(reply))
                reply = client.rpc_call("rpc_server", "testing")
                print("Got RPC reply: {}".format(reply))

    except KeyboardInterrupt:
        pass
