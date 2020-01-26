import functools
from threading import Lock

from pika.spec import Basic, BasicProperties

from .common_defs import EXCHANGE_TYPE_FANOUT, DEFAULT_EXCHANGE
from .producer_defs import Publish, RPCRequest, RPCResponse
from .log import LogClient


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
    Defines handling of a producer channel, enables confirm mode and keeps track
    of delivery tags with each sent message.
    """
    # general
    _log_client: LogClient

    _channel = None

    # confirm mode
    # Keeps track of messages since Confirm.SelectOK
    _expected_delivery_tag: int
    _delivery_tag_lock: Lock  # makes sure delivery tags are set securely
    _pending_confirm: dict  # messages that have not been confirmed yet

    _open = False

    def __init__(self, log_queue):
        """
        :param log_queue: IPC queue where logs shall be posted
        """
        self._log_client = LogClient(log_queue, RMQProducerChannel.__name__)
        self._log_client.debug("__init__")

        self._expected_delivery_tag = 0
        self._delivery_tag_lock = Lock()
        self._pending_confirm = dict()

    def open_channel(self, connection, notify_callback):
        """
        Opens a channel for the parameter connection.

        :param connection: connection to open channel for
        :param notify_callback: callback to notify once channel has been created
        """
        self._log_client.debug("open_channel")

        cb = functools.partial(self.on_channel_open,
                               notify_callback=notify_callback)

        if not self._open:
            connection.channel(on_open_callback=cb)

    def on_channel_open(self, channel, notify_callback=None):
        """
        Callback for when a channel has been established on the connection.

        :param pika.channel.Channel channel: the opened channel
        :param notify_callback: callback to notify once channel has been created
        """
        self._log_client.info("on_channel_open channel: {}".format(channel))

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
        :param Exception reason: exception explaining why the channel was closed
        """
        self._log_client.info("on_channel_closed channel: {} reason: {}"
                              .format(channel, reason))

        self._open = False

    def on_confirm_mode_activated(self, _frame, notify_callback=None):
        """
        Callback for when confirm mode has been activated.

        :param pika.frame.Method _frame: message frame
        :param notify_callback: callback to notify once channel has been created
        """
        self._log_client.debug("on_confirm_mode_activated frame: {}"
                               .format(_frame))

        notify_callback()

    def handle_work(self, work):
        """
        Handler function for work.

        :param work: work to be done by the producer channel
        """
        self._log_client.debug("handle_work work: {}".format(work))

        if max_attempts_reached(work):
            self._log_client.critical("handle_rpc_response max attempts "
                                      "reached for: {}".format(work))
            return

        if isinstance(work, Publish):
            self.handle_publish(work)
        elif isinstance(work, RPCRequest):
            self.handle_rpc_request(work)
        elif isinstance(work, RPCResponse):
            self.handle_rpc_response(work)

    def handle_publish(self, publish: Publish):
        """
        Handler for publishing work.

        :param Publish publish: information about a publish
        """
        self._log_client.debug("handle_publish")

        cb = functools.partial(self.on_exchange_declared,
                               publish=publish)
        self._channel.exchange_declare(exchange=publish.topic,
                                       exchange_type=EXCHANGE_TYPE_FANOUT,
                                       callback=cb)

    def handle_rpc_request(self, rpc_request: RPCRequest):
        """
        Handler for RPC request work.

        :param rpc_request: information about the RPC request
        """
        self._log_client.debug("handle_rpc_request")

        self.rpc_request(rpc_request)

    def handle_rpc_response(self, rpc_response: RPCResponse):
        """
        Handler for RPC response work.

        :param rpc_response: information about the RPC response
        """
        self._log_client.debug("handle_rpc_response")

        self.rpc_response(rpc_response)

    def on_exchange_declared(self, _frame, publish: Publish):
        """
        Callback for when an exchange has been declared.

        :param pika.frame.Method _frame: message frame
        :param str publish: additional parameter from functools.partial,
                            used to carry the publish object
        """
        self._log_client.debug("on_exchange_declared frame: {}"
                               .format(_frame))

        self.publish(publish)

    def publish(self, publish):
        """
        Perform a publish operation.

        :param Publish publish: the publish operation to perform
        """
        self._log_client.info("publish publish: {}".format(publish))

        self._pending_confirm.update(
            {self._delivery_tag(): publish.attempt()}
        )

        self._channel.basic_publish(
            exchange=publish.topic,
            routing_key="",
            body=publish.message,
        )

        self._log_client.debug("publish pending confirms: {}"
                               .format(self._pending_confirm))

    def rpc_request(self, rpc_request: RPCRequest):
        """
        Send an RPC request.

        :param rpc_request: request to send
        """
        self._log_client.info("rpc_request request: {}".format(rpc_request))

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

        self._log_client.debug("rpc_request pending confirms: {}"
                               .format(self._pending_confirm))

    def rpc_response(self, rpc_response: RPCResponse):
        """
        Send an RPC response.

        :param rpc_response: RPC response to send
        """
        self._log_client.info("rpc_response response: {}".format(rpc_response))

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

        self._log_client.debug("rpc_response pending confirms: {}"
                               .format(self._pending_confirm))

    def _delivery_tag(self):
        """
        Safely steps the delivery tag by using a Lock().

        :return: new delivery tag
        """
        self._log_client.info("_delivery_tag current tag: {}"
                               .format(self._expected_delivery_tag))

        self._delivery_tag_lock.acquire()
        self._expected_delivery_tag = self._expected_delivery_tag + 1
        delivery_tag = self._expected_delivery_tag

        self._log_client.info("_delivery_tag new tag: {}"
                              .format(self._expected_delivery_tag))

        self._delivery_tag_lock.release()

        return delivery_tag

    def on_delivery_confirmed(self, frame):
        """
        Callback for when a publish is confirmed

        :param pika.frame.Method frame: message frame, either a Basic.Ack or
                                        Basic.Nack
        """
        self._log_client.info("on_delivery_confirmed frame: {}".format(frame))

        confirm = self._pending_confirm.pop(frame.method.delivery_tag)

        if isinstance(frame.method, Basic.Nack):
            # Retry!
            self._log_client.error("on_delivery_confirmed delivery Nacked!")
            self.handle_work(confirm)

        self._log_client.debug("on_delivery_confirmed pending confirms: {}"
                               .format(self._pending_confirm))
