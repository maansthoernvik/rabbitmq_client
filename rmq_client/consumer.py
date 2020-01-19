from multiprocessing import Queue as IPCQueue, Process
from threading import Thread

from .log import LogClient
from .consumer_connection import create_consumer_connection
from .consumer_defs import *


class Consumer:
    """
    Used to package what makes up a consumer to the RMQConsumer class. This
    class is instantiated when a new subscription is made.

    The internal flag is used to subscribe internally in the RMQ client, where
    the raw message format (ConsumedMessage) is used instead of extracting only
    the delivered message.
    """
    callback: callable
    internal: bool  # Indicates that raw message shall be forwarded to consumer
    exchange: str
    routing_key: str

    def __init__(self, callback, internal=False, exchange="", routing_key=""):
        """
        :param callback: callback to call on new message
        :param internal: set if you want to preserve original message format:
                         ConsumedMessage instead of only getting bytes.
        :param exchange: subscribed exchange
        :param routing_key: subscribed routing key (queue name in case of
                            default exchange)
        """
        self.callback = callback
        self.internal = internal
        self.exchange = exchange
        self.routing_key = routing_key


class RMQConsumer:
    """
    Implements an asynchronous consumer for RabbitMQ.

    =================
    Pub/Sub
    =================
    Subscriptions can be added dynamically at any point, even before a
    connection has been established to the RMQ-server. The operation will simply
    be put on hold until exchanges/queues can be declared.

    =================
    RPC
    =================
    To support RPC, subscriptions are added with the internal flag, and the
    whole ConsumedMessage is forwarded to the RPCHandler. Other than that, the
    RPC support works by subscribing just like any other subscription, only that
    the recepient is the RPCHandler.
    """
    # general
    _monitoring_thread: Thread
    _log_client: LogClient

    # consumer list
    _consumers: list

    # IPC
    _connection_process: Process

    _work_queue: IPCQueue
    _consumed_messages: IPCQueue

    def __init__(self, log_queue):
        """
        :param log_queue: IPC queue to instantiate a log client
        """
        self._log_client = LogClient(log_queue, RMQConsumer.__name__)
        self._log_client.debug("__init__")

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
        self._log_client.info("start")

        self._connection_process = Process(
            target=create_consumer_connection,
            args=(self._work_queue,
                  self._consumed_messages,
                  self._log_client.get_log_queue())
        )
        self._connection_process.start()

        self._monitoring_thread = Thread(target=self.consume, daemon=True)
        self._monitoring_thread.start()

    def consume(self):
        """
        Recursively monitors the consumed_messages queue for any incoming
        messages.
        """
        self._log_client.debug("consume")

        message = self._consumed_messages.get()
        self.handle_message(message)
        self.consume()

    def handle_message(self, message: ConsumedMessage):
        """
        Defines handling for a received message, dispatches the message contents
        to registered callbacks depending on the topic.

        :param ConsumedMessage message: received message
        """
        self._log_client.info("handle_message message: {}".format(message))

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
        self._log_client.info("stop")

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
        self._log_client.debug("subscribe")

        self._consumers.append(Consumer(callback, exchange=topic))
        self._work_queue.put(Subscription(topic))

    def rpc_server(self, queue_name, callback):
        self._log_client.debug("rpc_server")

        self._consumers.append(Consumer(callback,
                                        internal=True,
                                        routing_key=queue_name))
        self._work_queue.put(RPCServer(queue_name))

    def rpc_client(self, queue_name, callback):
        self._log_client.debug("rpc_client")

        self._consumers.append(Consumer(callback,
                                        internal=True,
                                        routing_key=queue_name))
        self._work_queue.put(RPCClient(queue_name))
