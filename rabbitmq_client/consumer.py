import logging

from multiprocessing import Queue as IPCQueue, Process
from threading import Thread

from rabbitmq_client import log
from rabbitmq_client.log import LogManager

from rabbitmq_client.errors import ConsumerAlreadyExists

from rabbitmq_client.consumer_connection import create_consumer_connection
from rabbitmq_client.common_defs import Printable
from rabbitmq_client.consumer_defs import \
    Subscription, ConsumedMessage, ConsumeOk, StopConsumer, RPCServer, \
    RPCClient, CommandQueue


LOGGER = logging.getLogger(__name__)


class Consumer(Printable):
    """
    Used to package what makes up a consumer to the RMQConsumer class. This
    class is instantiated when a new subscription is made.

    The preserve flag is used to subscribe internally in the RMQ client, where
    the raw message format (ConsumedMessage) is used instead of extracting only
    the delivered message.
    """

    def __init__(self,
                 callback,
                 preserve=False,
                 exchange="",
                 routing_key="",
                 consumer_tag=""):
        """
        :param callback: callback to call on new message
        :param preserve: set if you want to preserve original message format:
                         ConsumedMessage instead of only getting bytes.
        :param exchange: subscribed exchange
        :param routing_key: subscribed routing key (queue name in case of
                            default exchange)
        """
        LOGGER.debug("__init__ Consumer")

        self.callback = callback
        self.preserve = preserve
        self.exchange = exchange
        self.routing_key = routing_key
        self.consumer_tag = consumer_tag

    def set_consumer_tag(self, new_consumer_tag):
        """
        Setter for consumer_tag

        :param new_consumer_tag: new consumer_tag
        :return: new consumer_tag
        """
        LOGGER.debug(f"setting consumer tag to: {new_consumer_tag}")

        self.consumer_tag = new_consumer_tag
        return self.consumer_tag


class RMQConsumer:
    """
    Implements an asynchronous consumer for RabbitMQ.

    =================
    Pub/Sub
    =================
    Subscriptions can be added dynamically at any point, even before a
    connection has been established to the RMQ-server. The operation will
    simply be put on hold until exchanges/queues can be declared.

    =================
    RPC
    =================
    To support RPC, subscriptions are added with the preserve flag, and the
    whole ConsumedMessage is forwarded to the RPCHandler. Other than that, the
    RPC support works by subscribing just like any other subscription, only
    that the recepient is the RPCHandler.
    """

    def __init__(self,
                 connection_parameters):
        """
        :param connection_parameters: pika.ConnectionParameters or None
        """
        LOGGER.debug("__init__")

        self._consumers = list()

        self._work_queue = IPCQueue()
        self._consumed_messages = IPCQueue()

        # This is fine since we're still in the same process!
        log_manager: LogManager = log.get_log_manager()

        self._connection_process = Process(
            target=create_consumer_connection,
            args=(
                connection_parameters,
                self._work_queue,
                self._consumed_messages,

                log_manager.log_queue,
                log_manager.log_level
            )
        )

        self._monitoring_thread = Thread(target=self.consume, daemon=True)

    def start(self):
        """
        Starts the RMQConsumer, meaning it is prepared for consuming messages.

        By starting the RMQConsumer, a process is created which will hold an
        RMQConsumerConnection. This function also starts a thread in the
        current process that monitors the consumed_messages queue for incoming
        messages.
        """
        LOGGER.info("start")

        self._connection_process.start()
        self._monitoring_thread.start()

    def stop(self):
        """
        Stops the RMQConsumer, tearing down the RMQConsumerConnection process.
        """
        LOGGER.info("stop")

        self._consumed_messages.put(StopConsumer())
        self._monitoring_thread.join()

        self._connection_process.terminate()
        self._connection_process.join(timeout=2)

    def add_new_consumer(self, new_consumer: Consumer):
        """
        Adds a new Consumer instance to the RMQConsumer's list of consumers, as
        long as the new consumer is not clashing with an old one.

        :param new_consumer: new consumer instance to add
        :raises ConsumerAlreadyExists: raised if the supplied consumer clashes
                                       with an existing one
        """
        matching_consumers = \
            [consumer for consumer in self._consumers
             if consumer.exchange == new_consumer.exchange and
             consumer.routing_key == new_consumer.routing_key]

        if matching_consumers:
            raise ConsumerAlreadyExists("A matching consumer already exists")

        self._consumers.append(new_consumer)

    def consume(self):
        """
        Monitors the consumed_messages queue for any incoming messages.
        """
        LOGGER.debug("consume")
        while True:
            LOGGER.debug("waiting for consumed messages")

            message = self._consumed_messages.get()
            LOGGER.debug("got new consumed message")

            # From Consumer connection
            if isinstance(message, ConsumedMessage):
                self.handle_message(message)
            elif isinstance(message, ConsumeOk):
                self.handle_consume_ok(message)

            # For stopping the monitoring thread
            elif isinstance(message, StopConsumer):
                self.flush_and_close_queues()
                break

    def flush_and_close_queues(self):
        """
        Flushed process shared queues in an attempt to stop background threads.
        """
        LOGGER.debug("flush_and_close_queues")
        while not self._consumed_messages.empty():
            self._consumed_messages.get()
        self._consumed_messages.close()
        # In order for client.stop() to be reliable and consistent, ensure
        # thread stop.
        self._consumed_messages.join_thread()

        while not self._work_queue.empty():
            self._work_queue.get()
        self._work_queue.close()
        # In order for client.stop() to be reliable and consistent, ensure
        # thread stop.
        self._work_queue.join_thread()

    def handle_message(self, message: ConsumedMessage):
        """
        Defines handling for a received message, dispatches the message
        contents to registered callbacks depending on the topic.

        :param ConsumedMessage message: received message
        """
        LOGGER.debug("handle_message message: {}".format(message))

        for consumer in self._consumers:
            if message.exchange == consumer.exchange and \
               message.routing_key == consumer.routing_key:
                if consumer.preserve:  # Internal consumer, forward object
                    consumer.callback(message)
                else:  # Only message content
                    consumer.callback(message.message)

    def handle_consume_ok(self, message: ConsumeOk):
        """
        Defines handling for a received ConsumeOk message, meaning a consume
        has been successfully started for a consumer.

        :param message: contains information about the started consumer
        """
        LOGGER.info(f"handle_consume_ok for: {message.consume.queue_name}")

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

        LOGGER.debug("handle_consume_ok new consumer state: {}"
                     .format(consumer_state))

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
        LOGGER.debug("subscribe")

        self.add_new_consumer(Consumer(callback, exchange=topic))
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
            if not consumer.preserve:  # RPC
                if consumer.exchange == topic and \
                   consumer.consumer_tag:  # If not "", ConsumeOK gotten
                    result = True
                    break

        LOGGER.debug("is_subscribed {}".format(result))

        return result

    def rpc_server(self, queue_name, callback):
        """
        Starts an RPC server by issuing a subscribe request to the consumer
        connection.

        The preserve flag ensures that the raw ConsumedMessage object is
        forwarded to the supplied callback function.

        :param queue_name: RPC server request queue name
        :param callback: callback on message received
        """
        LOGGER.debug("rpc_server")

        self.add_new_consumer(Consumer(callback,
                                       preserve=True,
                                       routing_key=queue_name))
        self._work_queue.put(RPCServer(queue_name))

    def rpc_client(self, queue_name, callback):
        """
        Starts an RPC client by issuing a subscribe request to the consumer
        connection.

        The preserve flag ensures that the raw ConsumedMessage object is
        forwarded to the supplied callback function.

        :param queue_name: RPC client response queue name
        :param callback: callback on message received
        """
        LOGGER.debug("rpc_client")

        self.add_new_consumer(Consumer(callback,
                                       preserve=True,
                                       routing_key=queue_name))
        self._work_queue.put(RPCClient(queue_name))

    def is_rpc_consumer_ready(self, server_name) -> bool:
        """
        Checks if the RPC server is ready.

        :return: True if ready
        """
        result = False

        for consumer in self._consumers:
            if consumer.preserve:
                if consumer.routing_key == server_name and \
                   consumer.consumer_tag:
                    result = True
                    break

        LOGGER.debug(f"is_rpc_consumer_ready {result}")

        return result

    def command_queue(self, queue_name, callback):
        """
        Adds a command queue instance to the list of consumers and issues a
        request to the consumer connection to set up the command queue.
        """
        LOGGER.debug(f"command_queue {queue_name}")

        self.add_new_consumer(Consumer(callback, routing_key=queue_name))
        self._work_queue.put(CommandQueue(queue_name))
