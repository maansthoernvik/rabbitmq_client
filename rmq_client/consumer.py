from multiprocessing import Queue as IPCQueue, Process
from threading import Thread

from .log import LogClient
from .consumer_connection import create_consumer_connection
from .consumer_defs import *


class Consumer(Printable):
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
    consumer_tag: str

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

    def set_consumer_tag(self, new_consumer_tag):
        """
        Setter for consumer_tag

        :param new_consumer_tag: new consumer_tag
        :return: new consumer_tag
        """
        self.consumer_tag = new_consumer_tag
        return self.consumer_tag


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
        Monitors the consumed_messages queue for any incoming messages.
        """
        while True:
            self._log_client.debug("consume waiting for new messages")

            message = self._consumed_messages.get()

            if isinstance(message, ConsumedMessage):
                self.handle_message(message)
            elif isinstance(message, ConsumeOk):
                self.handle_consume_ok(message)

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

    def handle_consume_ok(self, message: ConsumeOk):
        """
        Defines handling for a received ConsumeOk message, meaning a consume
        has been successfully started for a consumer.

        :param message: contains information about the started consumer
        """
        self._log_client.info("handle_consume_ok message: {}".format(message))

        if isinstance(message.consume, Subscription):
            # filter out subscription consumer and set consumer_tag
            [consumer.set_consumer_tag(message.consumer_tag)
             for consumer in self._consumers
             if consumer.exchange == message.consume.topic]
        else:  # RPCServer or RPCClient
            [consumer.set_consumer_tag(message.consumer_tag)
             for consumer in self._consumers
             if consumer.routing_key == message.consume.queue_name]

        consumer_state = ""
        for consumer in self._consumers:
            consumer_state = consumer_state + "\n\t{}".format(consumer)

        self._log_client.debug("handle_consume_ok new consumer state: {}"
                               .format(consumer_state))

    def stop(self):
        """
        Stops the RMQConsumer, tearing down the RMQConsumerConnection process.
        """
        self._log_client.info("stop")

        self._connection_process.terminate()
        self._connection_process.join(timeout=2)

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

    def is_subscribed(self, topic):
        """
        Checks if a subscription has been activated, meaning RMQ has confirmed
        with a ConsumeOk that a consume is active.

        :param topic: topic to check
        :return: true if active
        """
        result = False

        for consumer in self._consumers:
            if not consumer.internal:
                if consumer.exchange == topic and \
                   hasattr(consumer, 'consumer_tag'):
                    result = True
                    break

        self._log_client.debug("is_subscribed {}".format(result))

        return result

    def rpc_server(self, queue_name, callback):
        """
        Starts an RPC server by issuing a subscribe request to the consumer
        connection.

        The internal flag ensures that the raw ConsumedMessage object is
        forwarded to the supplied callback function.

        :param queue_name: RPC server request queue name
        :param callback: callback on message received
        """
        self._log_client.debug("rpc_server")

        self._consumers.append(Consumer(callback,
                                        internal=True,
                                        routing_key=queue_name))
        self._work_queue.put(RPCServer(queue_name))

    def is_rpc_consumer_ready(self, server_name) -> bool:
        """
        Checks if the RPC server is ready.

        :return: True if ready
        """
        result = False

        for consumer in self._consumers:
            if consumer.internal:
                if consumer.routing_key == server_name and \
                   hasattr(consumer, 'consumer_tag'):
                    result = True
                    break

        self._log_client.debug("is_rpc_consumer_ready {}".format(result))

        return result

    def rpc_client(self, queue_name, callback):
        """
        Starts an RPC client by issuing a subscribe request to the consumer
        connection.

        The internal flag ensures that the raw ConsumedMessage object is
        forwarded to the supplied callback function.

        :param queue_name: RPC client response queue name
        :param callback: callback on message received
        """
        self._log_client.debug("rpc_client")

        self._consumers.append(Consumer(callback,
                                        internal=True,
                                        routing_key=queue_name))
        self._work_queue.put(RPCClient(queue_name))
