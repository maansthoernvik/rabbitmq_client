import logging
import sys
import os
import signal
import threading

import pika

# For testing, needed to import "../rabbitmq_client".
sys.path.append(os.path.abspath(".."))

from rabbitmq_client.client import RMQClient  # noqa


"""
MANUAL TESTING TOOL
REMEMBER TO START FROM THE tests/ DIRECTORY!
"""


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

    print(f"Threads before test program start: {threading.active_count()}")

    print("instantiating RMQ client")
    credentials = pika.PlainCredentials('guest', 'guest')
    conn_params = pika.ConnectionParameters(host="localhost",
                                            port=5672,
                                            virtual_host='/',
                                            credentials=credentials)

    client = RMQClient(log_level=logging.DEBUG,
                       connection_parameters=conn_params)
    print("starting RMQ client")
    client.start()

    print(f"Threads after client start: {threading.active_count()}")

    def interrupt(frame, signal):
        print("Interrupting test program")
        client.stop()

        print(f"Threads after client stop: {threading.active_count()}")
        for t in threading.enumerate():
            print(t)

        raise Exception("Stopping")

    print("Registering INTERRUPT handler")
    signal.signal(signal.SIGINT, interrupt)

    print(f"Subscribe to {TEST_TOPIC_1} and {TEST_TOPIC_2}")
    client.subscribe(TEST_TOPIC_1, sub_callback)
    client.subscribe(TEST_TOPIC_2, sub_callback)

    print("Enabling RPC server to listen on rpc_server")
    client.enable_rpc_server("rpc_server", rpc_request_callback)

    print("Enabling RPC client")
    client.enable_rpc_client()

    def print_help():
        print("Possible inputs:")
        print("pub-1: Publish to TEST_TOPIC_1")
        print("pub-2: Publish to TEST_TOPIC_2")
        print("rpc-exists: RPC message on created RPC server")
        print("rpc-does-not-exist: to test timeouts")
        print("list-threads: lists currently running threads")
        print("stop: stops the client")
        print("")
    print_help()

    try:
        while True:
            inp = input()

            if inp == "pub-1":
                print(f"Publish to: {TEST_TOPIC_1}")
                client.publish(TEST_TOPIC_1, b"this is the message body")

            elif inp == "pub-2":
                print(f"Publish to: {TEST_TOPIC_2}")
                client.publish(TEST_TOPIC_2, b"this is the message body")

            elif inp == "rpc-exists":
                print("RPC call to: rpc_server")
                reply = client.rpc_call("rpc_server", b"testing RPC server")
                print("Got RPC reply: {}".format(reply))

            elif inp == "rpc-does-not-exist":
                print("RPC call to: rpc_server_2")
                reply = client.rpc_call("rpc_server_2",
                                        b"testing another RPC server")
                print("CLIENT got RPC reply: {}".format(reply))

            elif inp == "list-threads":
                print(f"Threads started: {threading.active_count()}")
                for t in threading.enumerate():
                    print(t)

            elif inp == "stop":
                print("Stopping client")
                client.stop()
                print(f"Threads after client stop: {threading.active_count()}")
                for t in threading.enumerate():
                    print(t)
                break

    except Exception:
        print("Exiting test loop")
