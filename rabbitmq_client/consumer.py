import logging

from threading import Thread

from rabbitmq_client.errors import ConsumerAlreadyExists

from rabbitmq_client.consumer_connection import RMQConsumerConnection
from rabbitmq_client.common_defs import Printable
from rabbitmq_client.consumer_defs import (
    Subscription,
    ConsumedMessage,
    ConsumeOk,
    RPCServer,
    RPCClient,
    CommandQueue
)


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
    Implements a consumer for RabbitMQ.
    """

    def __init__(self, connection_parameters=None):
        """
        :param connection_parameters: connection parameters to the RMQ server
        :type connection_parameters: pika.ConnectionParameters
        """
        LOGGER.debug("__init__")

        self._consumers = list()

        self._connection = RMQConsumerConnection(
            self._on_message,
            connection_parameters=connection_parameters
        )

        self._connection_thread = Thread(target=self.connect)

    def start(self):
        """
        Starts the RMQConsumer, meaning it is prepared for consuming messages.

        By starting the RMQConsumer, a process is created which will hold an
        RMQConsumerConnection. This function also starts a thread in the
        current process that monitors the consumed_messages queue for incoming
        messages.
        """
        LOGGER.info("start")

        self._connection_thread.start()

    def stop(self):
        """
        Stops the RMQConsumer, tearing down the RMQConsumerConnection process.
        """
        LOGGER.info("stop")

        self._connection.disconnect()
        self._connection_thread.join()

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

    def connect(self):
        """
        Initiates the underlying connection.
        """
        self._connection.connect()

    def _on_message(self, message):
        """
        """
        LOGGER.debug("got new consumed message")

        # From Consumer connection
        if isinstance(message, ConsumedMessage):
            self.handle_message(message)
        elif isinstance(message, ConsumeOk):
            self.handle_consume_ok(message)

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

        self._connection.subscribe(Subscription(topic))

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
        self._connection.rpc_server(RPCServer(queue_name))

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
        self._connection.rpc_client(RPCClient(queue_name))

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

        self._connection.command_queue(CommandQueue(queue_name))
