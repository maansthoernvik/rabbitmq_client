import logging

from rmq_client.client import RMQClient


TEST_TOPIC_1 = "test1"
TEST_TOPIC_2 = "test2"


def sub_callback(message):
    print("SUBSCRIBER got message: {}".format(message))


def rpc_request_callback(message):
    print("SERVER got RPC request: {}".format(message))

    response = "default RPC server response"

    return response


def rpc_request_callback_2(message):
    print("SERVER got RPC request: {}".format(message))

    response = "RPC SERVER NUMBER TWO RESPONSE"

    return response


if __name__ == "__main__":
    print("instantiating RMQ client")
    client = RMQClient(log_level=logging.DEBUG)
    print("starting RMQ client")
    client.start()

    client.subscribe(TEST_TOPIC_1, sub_callback)
    client.subscribe(TEST_TOPIC_2, sub_callback)

    client.enable_rpc_server("rpc_server", rpc_request_callback)
    #client.enable_rpc_server("rpc_server_2", rpc_request_callback_2)
    client.enable_rpc_client()

    try:
        while True:
            inp = input()

            if inp == "1":
                client.publish(TEST_TOPIC_1, b"this is the message body")

            elif inp == "2":
                client.publish(TEST_TOPIC_2, b"this is the message body")

            elif inp == "3":
                reply = client.rpc_call("rpc_server", b"testing RPC server")
                print("Got RPC reply: {}".format(reply))
            elif inp == "4":
                reply = client.rpc_call("rpc_server_2", b"testing another RPC server")
                print("CLIENT got RPC reply: {}".format(reply))

    except KeyboardInterrupt:
        pass
