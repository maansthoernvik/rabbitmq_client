import functools
import logging

from multiprocessing import Queue as IPCQueue
from threading import Lock

from pika.spec import Basic, BasicProperties

from .common_defs import EXCHANGE_TYPE_FANOUT
from .producer_defs import Publish, RPCRequest, RPCResponse
from .log import LogItem


def max_attempts_reached(work):
    if work.attempts > work.MAX_ATTEMPTS:
        return True
    return False


class RMQProducerChannel:
    # general
    _log_queue: IPCQueue

    _channel = None

    # confirm mode
    # Keeps track of messages since Confirm.SelectOK
    _expected_delivery_tag: int
    _delivery_tag_lock: Lock
    _pending_confirm: dict  # messages that have not been confirmed yet

    _open = False

    def __init__(self, log_queue):
        self._log_queue = log_queue
        self._log_queue.put(
            LogItem("__init__", RMQProducerChannel.__name__, level=logging.DEBUG)
        )

        self._expected_delivery_tag = 0
        self._delivery_tag_lock = Lock()
        self._pending_confirm = dict()

    def open_channel(self, connection, notify_callback):
        self._log_queue.put(
            LogItem("open_channel", RMQProducerChannel.__name__,
                    level=logging.DEBUG)
        )

        cb = functools.partial(self.on_channel_open,
                               notify_callback=notify_callback)

        if not self._open:
            connection.channel(on_open_callback=cb)

    def on_channel_open(self, channel, notify_callback=None):
        """
        Callback for when a channel has been established on the connection.

        :param pika.channel.Channel channel: the opened channel
        """
        self._log_queue.put(
            LogItem("on_channel_open channel: {}".format(channel),
                    RMQProducerChannel.__name__, level=logging.DEBUG)
        )

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
        self._log_queue.put(
            LogItem("on_channel_closed channel: {} reason: {}"
                    .format(channel, reason), RMQProducerChannel.__name__,
                    level=logging.DEBUG)
        )

    def on_confirm_mode_activated(self, _frame, notify_callback=None):
        """
        Callback for when confirm mode has been activated.

        :param pika.frame.Method _frame: message frame
        """
        self._log_queue.put(
            LogItem("on_confirm_mode_activated frame: {}".format(_frame),
                    RMQProducerChannel.__name__, level=logging.DEBUG)
        )

        notify_callback()

    def handle_work(self, work):
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
        self._log_queue.put(
            LogItem("handle_publish", RMQProducerChannel.__name__,
                    level=logging.DEBUG)
        )

        if max_attempts_reached(publish):
            # If max attempts reached, abort publish and write to critical log
            self._log_queue.put(
                LogItem("handle_publish max attempts exceeded",
                        RMQProducerChannel.__name__, level=logging.CRITICAL)
            )
            return

        cb = functools.partial(self.on_exchange_declared,
                               publish=publish)
        self._channel.exchange_declare(exchange=publish.topic,
                                       exchange_type=EXCHANGE_TYPE_FANOUT,
                                       callback=cb)

    def handle_rpc_request(self, rpc_request: RPCRequest):
        self._log_queue.put(
            LogItem("handle_rpc_request", RMQProducerChannel.__name__,
                    level=logging.DEBUG)
        )

        if max_attempts_reached(rpc_request):
            self._log_queue.put(
                LogItem("handle_rpc_request max attempts exceeded",
                        RMQProducerChannel.__name__, level=logging.CRITICAL)
            )
            return

        self.rpc_request(rpc_request)

    def handle_rpc_response(self, rpc_response: RPCResponse):
        self._log_queue.put(
            LogItem("handle_rpc_response", RMQProducerChannel.__name__,
                    level=logging.DEBUG)
        )

        if max_attempts_reached(rpc_response):
            self._log_queue.put(
                LogItem("handle_rpc_response max attempts exceeded",
                        RMQProducerChannel.__name__, level=logging.CRITICAL)
            )
            return

        self.rpc_response(rpc_response)

    def on_exchange_declared(self, _frame, publish: Publish):
        """
        Callback for when an exchange has been declared.

        :param pika.frame.Method _frame: message frame
        :param str publish: additional parameter from functools.partial,
                            used to carry the publish object
        """
        self._log_queue.put(
            LogItem("on_exchange_declared frame: {} publish: {}"
                    .format(_frame, publish), RMQProducerChannel.__name__,
                    level=logging.DEBUG)
        )
        self.publish(publish)

    def publish(self, publish):
        """
        Perform a publish operation.

        :param Publish publish: the publish operation to perform
        """
        self._log_queue.put(
            LogItem("publish: {}".format(publish),
                    RMQProducerChannel.__name__)
        )

        self._pending_confirm.update(
            {self._delivery_tag(): publish.attempt()}
        )

        self._channel.basic_publish(
            exchange=publish.topic,
            routing_key="",
            body=publish.message,
        )

        self._log_queue.put(
            LogItem("publish pending confirms: {}"
                    .format(self._pending_confirm),
                    RMQProducerChannel.__name__,
                    level=logging.DEBUG)
        )

    def rpc_request(self, rpc_request: RPCRequest):
        self._log_queue.put(
            LogItem("rpc_request: {}".format(rpc_request),
                    RMQProducerChannel.__name__)
        )

        self._pending_confirm.update(
            {self._delivery_tag(): rpc_request.attempt()}
        )

        self._channel.basic_publish(
            exchange="",
            routing_key=rpc_request.receiver,
            properties=BasicProperties(
                correlation_id=rpc_request.correlation_id,
                reply_to=rpc_request.reply_to
            ),
            body=rpc_request.message,
        )

        self._log_queue.put(
            LogItem("rpc_request pending confirms: {}"
                    .format(self._pending_confirm),
                    RMQProducerChannel.__name__,
                    level=logging.DEBUG)
        )

    def rpc_response(self, rpc_response: RPCResponse):
        self._log_queue.put(
            LogItem("rpc_response: {}".format(rpc_response),
                    RMQProducerChannel.__name__)
        )

        self._pending_confirm.update(
            {self._delivery_tag(): rpc_response.attempt()}
        )

        self._channel.basic_publish(
            exchange="",
            routing_key=rpc_response.receiver,
            properties=BasicProperties(
                correlation_id=rpc_response.correlation_id
            ),
            body=rpc_response.message,
        )

        self._log_queue.put(
            LogItem("rpc_response pending confirms: {}"
                    .format(self._pending_confirm),
                    RMQProducerChannel.__name__,
                    level=logging.DEBUG)
        )

    def _delivery_tag(self):
        self._delivery_tag_lock.acquire()
        self._expected_delivery_tag = self._expected_delivery_tag + 1
        delivery_tag = self._expected_delivery_tag

        self._delivery_tag_lock.release()

        return delivery_tag

    def on_delivery_confirmed(self, frame):
        """
        Callback for when a publish is confirmed

        :param pika.frame.Method frame: message frame, either a Basic.Ack or
                                        Basic.Nack
        """
        self._log_queue.put(
            LogItem("on_delivery_confirmed frame: {}".format(frame),
                    RMQProducerChannel.__name__, level=logging.DEBUG)
        )
        confirm = self._pending_confirm.pop(frame.method.delivery_tag)

        if isinstance(frame.method, Basic.Nack):
            # Retry!
            self._log_queue.put(
                LogItem("on_delivery_confirmed failed delivery".format(frame),
                        RMQProducerChannel.__name__, level=logging.WARNING)
            )
            self.handle_work(confirm)

        self._log_queue.put(
            LogItem("on_delivery_confirmed pending confirms: {}"
                    .format(self._pending_confirm),
                    RMQProducerChannel.__name__, level=logging.DEBUG)
        )
