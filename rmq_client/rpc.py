import logging
import uuid

from multiprocessing import Queue as IPCQueue
from threading import Event

from .defs import Subscription, RPCReply, ConsumedMessage
from .log import LogItem
from .consumer import RMQConsumer
from .producer import RMQProducer

# RPC modes available for the RMQRPCHandler
SERVER = 0
CLIENT = 1
BOTH = 2
MODES = [SERVER, CLIENT, BOTH]

RPC_REPLY_PREFIX = "RPC-REPLY-"


class RPCResponse:

    blocker: Event
    response: str

    def __init__(self):
        self.blocker = Event()
        self.response = "NONE"

    def __str__(self):
        return "{} {}".format(self.__class__, self.__dict__)


class RMQRPCHandler:

    _log_queue: IPCQueue

    _consumer: RMQConsumer
    _producer: RMQProducer

    # RPC Server
    _rpc_server_name: str = None
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
        # Start a server-only RPC handler
        # 3. Subscribe to an owned request queue
        # 2. Use the producer to send replies to a caller-named reply queue

        # Start a client-only RPC handler
        # 1. Use the producer to send requests to named RPC queues
        # 2. Subscribe to an owned reply queue

        # Start a both client and server RPC handler, meaning it can both
        # accept and send RPC requests
        # 1. Use the producer to send requests to named RPC queues
        # 2. Subscribe to an owned reply queue
        # 3. Subscribe to an owned request queue
        # 4. Use the producer to send replies to a caller-named reply queue

    def stop(self):
        pass

    def enable_rpc_server(self, rpc_server_name, rpc_request_callback):
        if self._rpc_server_name:
            return

        self._rpc_server_name = rpc_server_name
        self._rpc_request_callback = rpc_request_callback
        self._consumer.subscribe(self._rpc_server_name,
                                 self.handle_rpc_request,
                                 sub_type=Subscription.RPC_REQUEST)

    def enable_rpc_client(self):
        if self._reply_queue:
            return

        self._pending_requests = dict()
        self._reply_queue = RPC_REPLY_PREFIX + str(uuid.uuid1())
        self._consumer.subscribe(self._reply_queue,
                                 self.handle_rpc_reply,
                                 sub_type=Subscription.RPC_REPLY)

    def rpc_call(self, receiver, message):
        self._log_queue.put(
            LogItem("rpc_call",
                    RMQRPCHandler.__name__,
                    level=logging.DEBUG)
        )
        corr_id = str(uuid.uuid1())

        response = RPCResponse()
        self._pending_requests.update({corr_id: response})

        self._producer.publish(receiver,
                               message,
                               correlation_id=corr_id,
                               reply_to=self._reply_queue)

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
        answer = self._rpc_request_callback(message.message_content)

        self._producer.publish(message.reply_to,
                               answer,
                               correlation_id=message.correlation_id)

    def handle_rpc_reply(self, reply: RPCReply):
        self._log_queue.put(
            LogItem("handle_rpc_reply reply: {}".format(reply),
                    RMQRPCHandler.__name__,
                    level=logging.DEBUG)
        )
        response: RPCResponse = self._pending_requests.pop(reply.correlation_id)
        response.response = reply.message_content
        response.blocker.set()

    def rpc_cast(self, receiver, message, callback):
        pass
