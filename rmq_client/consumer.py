from enum import Enum
from multiprocessing import Queue as IPCQueue, Process
from threading import Thread

from .defs import Subscription, Message
from .consumer_connection import create_consumer_connection


class RMQConsumer:
    """
    Implements an asynchronous consumer for RabbitMQ.

    =================
    Pub/Sub
    =================
    Subscriptions can be added dynamically at any point, even before a
    connection has been established to the RMQ-server. The operation will simply
    be put on hold until exchanges/queues can be declared.
    """

    # Pub/sub
    _topic_callbacks: dict

    # IPC
    _connection_process: Process

    _work_queue: IPCQueue
    _consumed_messages: IPCQueue

    def __init__(self):
        """
        Initializes the RMQConsumer's IPC queues.
        """
        print("consumer __init__")
        self._topic_callbacks = dict()

        self._work_queue = IPCQueue()
        self._consumed_messages = IPCQueue()

    def start(self):
        """
        By starting the RMQConsumer, a process is created which will hold an
        RMQConsumerConnection. This function also starts a thread in the current
        process that monitors the consumed_messages queue for incoming messages.
        """
        print("consumer start()")
        self._connection_process = Process(
            target=create_consumer_connection,
            args=(self._work_queue, self._consumed_messages)
        )
        self._connection_process.start()

        thread = Thread(target=self.consume, daemon=True)
        thread.start()

    def consume(self):
        """
        Recursively monitors the consumed_messages queue for any incoming
        messages.
        """
        print("consumer consume()")
        message = self._consumed_messages.get()
        self.handle_message(message)
        self.consume()

    def handle_message(self, message: Message):
        """
        Defines handling for a received message, dispatches the message contents
        to registered callbacks.

        :param message:
        """
        print("consumer got message: {}".format(message))
        self._topic_callbacks.get(message.topic)(message.message_content)

    def stop(self):
        """
        Stops the RMQConsumer, tearing down the RMQConsumerConnection process.
        """
        print("consumer stop()")
        self._connection_process.terminate()

    def subscribe(self, topic, callback):
        """
        Subscribes to messages sent to the named topic. Messages received on
        this topic will be dispatched to the provided callback.

        :param topic:
        :param callback:
        """
        # 1. Add callback to be called when event on that topic + routing_key
        # 2. Request a subscription on the new topic towards the consumer
        #    connection
        print("consumer subscribe()")
        self._work_queue.put(Subscription(topic=topic))
        self._topic_callbacks.update({topic: callback})
