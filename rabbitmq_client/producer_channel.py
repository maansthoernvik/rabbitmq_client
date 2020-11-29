import functools
import logging

from threading import Lock

from pika.spec import Basic, BasicProperties

from rabbitmq_client.common_defs import EXCHANGE_TYPE_FANOUT
from rabbitmq_client.producer_defs import Publish, RPCRequest, RPCResponse, \
                                          Command, DEFAULT_EXCHANGE


LOGGER = logging.getLogger(__name__)


def max_attempts_reached(work):
    """
    Checks if max attemts have been reached.

    :param work: true if work.attempts > work.MAX_ATTEMPTS
    """
    if work.attempts > work.MAX_ATTEMPTS:
        return True
    return False


class RMQProducerChannel:
    """
    Defines handling of a producer channel, enables confirm mode and keeps
    track of delivery tags with each sent message.
    """

    def __init__(self):
        """"""
        LOGGER.debug("__init__")

        self._expected_delivery_tag = 0
        self._delivery_tag_lock = Lock()
        self._pending_confirm = dict()

        self._channel = None
        self._open = False

    def open_channel(self, connection, notify_callback):
        """
        Opens a channel for the parameter connection.

        :param connection: connection to open channel for
        :param notify_callback: callback to notify once channel has been
                                created
        """
        LOGGER.debug("open_channel")

        cb = functools.partial(self.on_channel_open,
                               notify_callback=notify_callback)

        if not self._open:
            connection.channel(on_open_callback=cb)

    def on_channel_open(self, channel, notify_callback=None):
        """
        Callback for when a channel has been established on the connection.

        :param pika.channel.Channel channel: the opened channel
        :param notify_callback: callback to notify once channel has been
                                created
        """
        LOGGER.info("on_channel_open channel: {}".format(channel))

        self._open = True

        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)

        cb = functools.partial(self.on_confirm_mode_activated,
                               notify_callback=notify_callback)
        self._channel.confirm_delivery(
            ack_nack_callback=self.on_delivery_confirmed,
            callback=cb
        )

    def on_channel_closed(self, channel, reason):
        """
        Callback for when a channel has been closed.

        :param pika.channel.Channel channel: the channel that was closed
        :param Exception reason: exception explaining why the channel was
                                 closed
        """
        LOGGER.info(f"on_channel_closed channel: {channel} reason: {reason}")

        self._open = False

    def on_confirm_mode_activated(self, _frame, notify_callback=None):
        """
        Callback for when confirm mode has been activated.

        :param pika.frame.Method _frame: message frame
        :param notify_callback: callback to notify once channel has been
                                created
        """
        LOGGER.debug(f"on_confirm_mode_activated frame: {_frame}")

        notify_callback()

    def handle_produce(self, produce):
        """
        Handler function for produce work.

        :param produce: work to be done by the producer channel
        :type produce: Publish | RPCRequest | RPCResponse | Command
        """
        LOGGER.debug("handle_produce work: {}".format(produce))

        if max_attempts_reached(produce):
            LOGGER.critical(f"handle_rpc_response max attempts "
                            f"reached for: {produce}, aborting produce")
            return

        if isinstance(produce, Publish):
            cb = functools.partial(self.on_exchange_declared,
                                   produce=produce)
            self._channel.exchange_declare(exchange=produce.topic,
                                           exchange_type=EXCHANGE_TYPE_FANOUT,
                                           callback=cb)

        elif isinstance(produce, RPCRequest):
            cb = functools.partial(self.on_queue_declared,
                                   produce=produce)
            self._channel.queue_declare(queue=produce.receiver,
                                        durable=True,  # Survive broker reboot
                                        callback=cb)

        elif isinstance(produce, RPCResponse):
            # Since just got RPC request, send RPC response without checking
            # for queue declaration. Besides, the RPC reply queue is exclusive.
            self.rpc_response(produce)

        elif isinstance(produce, Command):
            cb = functools.partial(self.on_queue_declared,
                                   produce=produce)
            self._channel.queue_declare(queue=produce.command_queue,
                                        durable=True,  # Survive broker reboot
                                        callback=cb)

    def on_exchange_declared(self, _frame, produce):
        """
        Callback for when an exchange has been declared.

        :param pika.frame.Method _frame: message frame
        :param produce: additional parameter from functools.partial,
                        used to carry the publish object
        :type produce: Publish
        """
        LOGGER.debug(f"on_exchange_declared frame: {_frame}")

        self.publish(produce)

    def on_queue_declared(self, frame, produce):
        """
        Callback for when a queue has been declared.

        :param pika.frame.Method frame: message frame
        :param produce: information about the ongoing produce
        :type produce: RPCRequest | RPCResponse | Command
        """
        LOGGER.debug(f"on_queue_declared frame: {frame} produce: {produce}")

        # RPC responses do not check for queue declaration, nor do publishes.
        if isinstance(produce, RPCRequest):
            self.rpc_request(produce)
        elif isinstance(produce, Command):
            self.command(produce)

    def publish(self, publish):
        """
        Perform a publish operation.

        :param publish: the publish operation to perform
        :type publish: Publish
        """
        LOGGER.info("publish publish: {}".format(publish))

        self._pending_confirm.update(
            {self._delivery_tag(): publish.attempt()}
        )

        self._channel.basic_publish(
            exchange=publish.topic,
            routing_key="",
            body=publish.message,
        )

        LOGGER.debug("publish pending confirms: {}"
                     .format(self._pending_confirm))

    def rpc_request(self, rpc_request):
        """
        Send an RPC request.

        :param rpc_request: request to send
        :type rpc_request: RPCRequest
        """
        LOGGER.info("rpc_request request: {}".format(rpc_request))

        self._pending_confirm.update(
            {self._delivery_tag(): rpc_request.attempt()}
        )

        self._channel.basic_publish(
            exchange=DEFAULT_EXCHANGE,
            routing_key=rpc_request.receiver,
            properties=BasicProperties(
                correlation_id=rpc_request.correlation_id,
                reply_to=rpc_request.reply_to
            ),
            body=rpc_request.message,
        )

        LOGGER.debug(f"rpc_request pending confirms: {self._pending_confirm}")

    def rpc_response(self, rpc_response):
        """
        Send an RPC response.

        :param rpc_response: RPC response to send
        :type rpc_response: RPCResponse
        """
        LOGGER.info(f"rpc_response response: {rpc_response}")

        self._pending_confirm.update(
            {self._delivery_tag(): rpc_response.attempt()}
        )

        self._channel.basic_publish(
            exchange=DEFAULT_EXCHANGE,
            routing_key=rpc_response.receiver,
            properties=BasicProperties(
                correlation_id=rpc_response.correlation_id
            ),
            body=rpc_response.message,
        )

        LOGGER.debug(f"rpc_response pending confirms: {self._pending_confirm}")

    def command(self, command):
        """
        Send a Command.

        :param command: command work item
        :type command: Command
        """
        LOGGER.info(f"command: {command}")

        self._pending_confirm.update(
            {self._delivery_tag(): command.attempt()}
        )

        self._channel.basic_publish(
            exchange=DEFAULT_EXCHANGE,
            routing_key=command.command_queue,
            body=command.command
        )

        LOGGER.debug(f"command pending confirms: {self._pending_confirm}")

    def _delivery_tag(self):
        """
        Safely steps the delivery tag by using a Lock().

        :return: new delivery tag
        """
        LOGGER.info(f"_delivery_tag current tag: "
                    f"{self._expected_delivery_tag}")

        self._delivery_tag_lock.acquire()
        self._expected_delivery_tag = self._expected_delivery_tag + 1
        delivery_tag = self._expected_delivery_tag

        LOGGER.info(f"_delivery_tag new tag: {self._expected_delivery_tag}")

        self._delivery_tag_lock.release()

        return delivery_tag

    def on_delivery_confirmed(self, frame):
        """
        Callback for when a publish is confirmed

        :param pika.frame.Method frame: message frame, either a Basic.Ack or
                                        Basic.Nack
        """
        LOGGER.info("on_delivery_confirmed frame: {}".format(frame))

        confirm = self._pending_confirm.pop(frame.method.delivery_tag)

        if isinstance(frame.method, Basic.Nack):
            # Retry!
            LOGGER.error("on_delivery_confirmed delivery Nacked!")
            self.handle_work(confirm)

        LOGGER.debug(f"on_delivery_confirmed pending confirms: "
                     f"{self._pending_confirm}")
