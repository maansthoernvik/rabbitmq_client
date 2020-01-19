import logging
import uuid

from multiprocessing import Queue as IPCQueue
from threading import Event

from .common_defs import Printable
from .consumer_defs import ConsumedMessage
from .log import LogItem
from .consumer import RMQConsumer
from .producer import RMQProducer


RPC_REPLY_PREFIX = "RPC-REPLY-"


class RPCResponse(Printable):

    blocker: Event
    response: str

    def __init__(self):
        self.blocker = Event()
        self.response = "NONE"


class RMQRPCHandler:

    _log_queue: IPCQueue

    _consumer: RMQConsumer
    _producer: RMQProducer

    # RPC Server
    _rpc_queue_name: str = None
    _rpc_request_callback: callable

    # RPC Client
    _reply_queue: str = None
    _pending_requests: dict

    def __init__(self, consumer, producer, log_queue):
        self._log_queue = log_queue
        self._log_queue.put(
            LogItem("__init__", RMQRPCHandler.__name__, level=logging.DEBUG)
        )

        self._consumer = consumer
        self._producer = producer

    def start(self):
        self._log_queue.put(
            LogItem("start", RMQRPCHandler.__name__)
        )

    def stop(self):
        pass

    def enable_rpc_server(self, rpc_queue_name, rpc_request_callback):
        if self._rpc_queue_name:
            return

        self._rpc_queue_name = rpc_queue_name
        self._rpc_request_callback = rpc_request_callback
        self._consumer.rpc_server(self._rpc_queue_name,
                                  self.handle_rpc_request)

    def enable_rpc_client(self):
        if self._reply_queue:
            return

        self._pending_requests = dict()
        self._reply_queue = RPC_REPLY_PREFIX + str(uuid.uuid1())
        self._consumer.rpc_client(self._reply_queue, self.handle_rpc_reply)

    def rpc_call(self, receiver, message):
        self._log_queue.put(
            LogItem("rpc_call",
                    RMQRPCHandler.__name__,
                    level=logging.DEBUG)
        )
        corr_id = str(uuid.uuid1())

        response = RPCResponse()
        self._pending_requests.update({corr_id: response})

        self._producer.rpc_request(receiver,
                                   message,
                                   corr_id,
                                   self._reply_queue)

        self._log_queue.put(
            LogItem("blocking waiting for response".format(response.response),
                    RMQRPCHandler.__name__,
                    level=logging.DEBUG)
        )
        response.blocker.wait(timeout=2.0)

        self._log_queue.put(
            LogItem("rpc_call response: {}".format(response.response),
                    RMQRPCHandler.__name__,
                    level=logging.DEBUG)
        )
        return response.response

    def handle_rpc_request(self, message: ConsumedMessage):
        self._log_queue.put(
            LogItem("handle_rpc_request request: {}".format(message),
                    RMQRPCHandler.__name__,
                    level=logging.DEBUG)
        )
        answer = self._rpc_request_callback(message.message)

        self._producer.rpc_reply(message.reply_to,
                                 answer,
                                 correlation_id=message.correlation_id)

    def handle_rpc_reply(self, message: ConsumedMessage):
        self._log_queue.put(
            LogItem("handle_rpc_reply reply: {}".format(message),
                    RMQRPCHandler.__name__,
                    level=logging.DEBUG)
        )
        response: RPCResponse = \
            self._pending_requests.pop(message.correlation_id)
        response.response = message.message
        response.blocker.set()

    def rpc_cast(self, receiver, message, callback):
        pass
