import logging

from multiprocessing import Queue as IPCQueue, Process
from threading import Thread

from .log import LogItem
from .consumer_connection import create_consumer_connection
from .consumer_defs import *


class Consumer:
    callback: callable
    internal: bool  # Indicates that raw message shall be forwarded to consumer
    exchange: str
    routing_key: str

    def __init__(self, callback, internal=False, exchange="", routing_key=""):
        self.callback = callback
        self.internal = internal
        self.exchange = exchange
        self.routing_key = routing_key


class RMQConsumer:
    """
    Class RMQConsumer

    Implements an asynchronous consumer for RabbitMQ.

    =================
    Pub/Sub
    =================
    Subscriptions can be added dynamically at any point, even before a
    connection has been established to the RMQ-server. The operation will simply
    be put on hold until exchanges/queues can be declared.
    """
    # general
    _monitoring_thread: Thread
    _log_queue: IPCQueue

    _consumers: list

    # IPC
    _connection_process: Process

    _work_queue: IPCQueue
    _consumed_messages: IPCQueue

    def __init__(self, log_queue):
        """
        Initializes the RMQConsumer's member variables
        """
        self._log_queue = log_queue
        self._log_queue.put(
            LogItem("__init__", RMQConsumer.__name__, level=logging.DEBUG)
        )

        self._consumers = list()

        self._work_queue = IPCQueue()
        self._consumed_messages = IPCQueue()

    def start(self):
        """
        Starts the RMQConsumer, meaning it is prepared for consuming messages.

        By starting the RMQConsumer, a process is created which will hold an
        RMQConsumerConnection. This function also starts a thread in the current
        process that monitors the consumed_messages queue for incoming messages.
        """
        self._log_queue.put(
            LogItem("start", RMQConsumer.__name__)
        )
        self._connection_process = Process(
            target=create_consumer_connection,
            args=(self._work_queue, self._consumed_messages, self._log_queue)
        )
        self._connection_process.start()

        self._monitoring_thread = Thread(target=self.consume, daemon=True)
        self._monitoring_thread.start()

    def consume(self):
        """
        Recursively monitors the consumed_messages queue for any incoming
        messages.
        """
        self._log_queue.put(
            LogItem("consume", RMQConsumer.__name__, level=logging.DEBUG)
        )
        message = self._consumed_messages.get()
        self.handle_message(message)
        self.consume()

    def handle_message(self, message: ConsumedMessage):
        """
        Defines handling for a received message, dispatches the message contents
        to registered callbacks depending on the topic.

        :param ConsumedMessage message: received message
        """
        self._log_queue.put(
            LogItem("handle_message got: {}".format(message),
                    RMQConsumer.__name__)
        )
        for consumer in self._consumers:
            if message.exchange == consumer.exchange and \
                    message.routing_key == consumer.routing_key:
                if consumer.internal:  # Internal consumer, forward raw
                    consumer.callback(message)
                else:  # Only message content
                    consumer.callback(message.message)

    def stop(self):
        """
        Stops the RMQConsumer, tearing down the RMQConsumerConnection process.
        """
        self._log_queue.put(
            LogItem("stop", RMQConsumer.__name__)
        )
        self._connection_process.terminate()

    def subscribe(self, topic, callback):
        """
        Subscribes to messages sent to the named topic. Messages received on
        this topic will be dispatched to the provided callback.

            callback(message: bytes)

        Updates the internal dictionary topic_callback so that the input
        callback will be called when a message is received for the given topic.

        :param str topic: topic to subscribe to
        :param callable callback: callback on message received
        """
        # 1. Add callback to be called when event on that topic + routing_key
        # 2. Request a subscription on the new topic towards the consumer
        #    connection
        self._log_queue.put(
            LogItem("subscribe", RMQConsumer.__name__, level=logging.DEBUG)
        )

        self._consumers.append(Consumer(callback, exchange=topic))
        self._work_queue.put(Subscription(topic))

    def rpc_server(self, queue_name, callback):
        self._log_queue.put(
            LogItem("rpc_server", RMQConsumer.__name__, level=logging.DEBUG)
        )

        self._consumers.append(Consumer(callback,
                                        internal=True,
                                        routing_key=queue_name))
        self._work_queue.put(RPCServer(queue_name))

    def rpc_client(self, queue_name, callback):
        self._log_queue.put(
            LogItem("rpc_client", RMQConsumer.__name__, level=logging.DEBUG)
        )

        self._consumers.append(Consumer(callback,
                                        internal=True,
                                        routing_key=queue_name))
        self._work_queue.put(RPCClient(queue_name))
