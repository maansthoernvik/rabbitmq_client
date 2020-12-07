import logging
import sys
import os
import signal
import threading
import time
import argparse

import pika

from multiprocessing import Queue
from logging import StreamHandler
from logging.handlers import QueueHandler, QueueListener

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

    response = b'default RPC server response'

    return response


def rpc_request_callback_2(message):
    print("SERVER got RPC request: {}".format(message))

    response = "RPC SERVER NUMBER TWO RESPONSE"

    return response


def command_queue_callback(message):
    print(f"command gotten: {message}")


def parse_arguments():
    parser = argparse.ArgumentParser(description="Manual testing tool")
    parser.add_argument('-l', '--logging', action='store_true')
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()

    print(f"Threads before test program start: {threading.active_count()}")

    print("instantiating RMQ client")
    credentials = pika.PlainCredentials('guest', 'guest')
    conn_params = pika.ConnectionParameters(host="localhost",
                                            port=5672,
                                            virtual_host='/',
                                            credentials=credentials)

    if args.logging:
        log_queue = Queue()

        # Current process handler
        logger = logging.getLogger("rabbitmq_client")
        logger.setLevel(logging.DEBUG)
        queue_handler = QueueHandler(log_queue)
        queue_handler.setLevel(logging.DEBUG)
        logger.addHandler(queue_handler)

        # Final logging destination
        stream_handler = StreamHandler()
        stream_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(fmt="{asctime} {levelname:^8} "
                                          "{name} {message}",
                                      style="{",
                                      datefmt="%d/%m/%Y %H:%M:%S")
        stream_handler.setFormatter(formatter)
        listener = QueueListener(
            log_queue,
            stream_handler,
            respect_handler_level=True
        )
        listener.start()
    else:
        log_queue = None

    client = RMQClient(log_queue=log_queue,
                       connection_parameters=conn_params)
    print("starting RMQ client")
    client.start()
    time.sleep(1)

    print(f"Threads after client start: {threading.active_count()}")

    def interrupt(frame, signal):
        print("Interrupting test program")
        stop()
        raise Exception("Stopping")

    def stop():
        print("Stopping test program")
        client.stop()

        if args.logging:
            print("Logging activated, stopping queue and listener")
            listener.stop()
            while not log_queue.empty():
                log_queue.get()
            log_queue.close()
            log_queue.join_thread()

        print(f"Threads after client stop: {threading.active_count()}")
        for t in threading.enumerate():
            print(t)

    print("Registering INTERRUPT handler")
    signal.signal(signal.SIGINT, interrupt)

    print(f"Subscribe to {TEST_TOPIC_1} and {TEST_TOPIC_2}")
    client.subscribe(TEST_TOPIC_1, sub_callback)
    client.subscribe(TEST_TOPIC_2, sub_callback)

    print("Enabling RPC server to listen on rpc_server")
    client.enable_rpc_server("rpc_server", rpc_request_callback)

    print("Enabling RPC client")
    client.enable_rpc_client()

    print("Setting up a command queue")
    client.command_queue("command_queue", command_queue_callback)

    def print_help():
        print("")
        print("### Possible inputs ###")
        print("pub-1: Publish to TEST_TOPIC_1")
        print("pub-2: Publish to TEST_TOPIC_2")
        print("rpc-exists: RPC message on created RPC server")
        print("rpc-does-not-exist: to test timeouts")
        print("command: to test sending a command to command_queue")
        print("list-threads: lists currently running threads")
        print("stop: stops the client")
        print("")
    print_help()

    try:
        while True:
            print("#########################")
            print("Awaiting input...")
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

            elif inp == "command":
                print("Command being sent to command_queue")
                client.command("command_queue", "command message")

            elif inp == "list-threads":
                print(f"Threads started: {threading.active_count()}")
                for t in threading.enumerate():
                    print(t)

            elif inp == "stop":
                print("Stopping client")
                stop()
                break

    except Exception:
        print("Exiting test loop")
