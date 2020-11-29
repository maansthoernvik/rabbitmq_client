import uuid
import logging

from threading import Event

from .common_defs import Printable
from .consumer_defs import ConsumedMessage


LOGGER = logging.getLogger(__name__)

RPC_REPLY_PREFIX = "RPC-REPLY-"

RPC_DEFAULT_REPLY = b'NONE'


class RPCResponse(Printable):
    """
    RPC response object spec.
    """

    blocker: Event  # used to wait for response
    response: bytes  # defaults to RPC_DEFAULT_REPLY if no response in time

    def __init__(self):
        """"""
        self.blocker = Event()
        self.response = RPC_DEFAULT_REPLY


class RMQRPCHandler:
    """
    RPC handler which uses the consumer and producer instances to handle RPC
    requests and responses.
    """

    def __init__(self, consumer, producer):
        """
        :param consumer: consumer instance
        :param producer: producer instanse
        """
        LOGGER.debug("__init__")

        self._consumer = consumer
        self._producer = producer
        self._response_queue_name = None
        self._request_queue_name = None
        self._rpc_request_callback = None
        self._pending_requests = dict()

    def start(self):
        """
        Not needed, yet.
        """
        pass

    def stop(self):
        """
        Not needed, yet.
        """
        pass

    def enable_rpc_server(self, rpc_queue_name, rpc_request_callback):
        """
        Enables an RPC server, which can accept requests and respond to them.
        The RPC handler will subscribe to the queue name and expect the
        provided callback to RETURN a value which it can reply with.

            rpc_request_callback(message: bytes) -> bytes

         !!! NOTE The importance of the supplied callback to RETURN bytes. !!!

        :param rpc_queue_name: name of RPC request queue to subscribe to
        :param rpc_request_callback: callback to issue requests to
        """
        LOGGER.debug("enable_rpc_server")

        if self._request_queue_name:
            LOGGER.warning("enable_rpc_server an RPC server has"
                           "already been declared")
            return

        self._request_queue_name = rpc_queue_name
        self._rpc_request_callback = rpc_request_callback
        self._consumer.rpc_server(self._request_queue_name,
                                  self.handle_rpc_request)

    def is_rpc_server_ready(self) -> bool:
        """
        Checks if the RPC server is ready, meaning it is consuming on the RPC
        server queue.

        :return: True if ready
        """
        LOGGER.debug("is_rpc_server_ready")

        # If no request queue exists, definitively not ready.
        if not self._request_queue_name:
            return False

        return self._consumer.is_rpc_consumer_ready(self._request_queue_name)

    def enable_rpc_client(self):
        """
        Enables the client to act as an RPC client. This will establish a reply
        queue to receive responses to sent RPC requests.
        """
        LOGGER.debug("enable_rpc_client")

        if self._response_queue_name:
            return

        self._response_queue_name = RPC_REPLY_PREFIX + str(uuid.uuid1())
        self._consumer.rpc_client(self._response_queue_name,
                                  self.handle_rpc_response)

    def is_rpc_client_ready(self) -> bool:
        """
        Check if the RPC client is ready, meaning it is consuming on the RPC
        client's reply queue.

        :return: True if ready
        """
        LOGGER.debug("is_rpc_client_ready")

        # If no response queue exists, definitively not ready.
        if not self._response_queue_name:
            return False

        return self._consumer.is_rpc_consumer_ready(self._response_queue_name)

    def rpc_call(self, receiver, message) -> bytes:
        """
        NOTE! Must enable_rpc_client before making calls to this function.

        Make a synchronous call to an RPC server.

        :param str receiver: name of the RPC server to send the request to
        :param bytes message: message to send to the RPC server

        :return bytes answer: response message from the RPC server
        """
        LOGGER.debug("rpc_call")

        corr_id = str(uuid.uuid1())

        response = RPCResponse()
        self._pending_requests.update({corr_id: response})

        self._producer.rpc_request(receiver,
                                   message,
                                   corr_id,
                                   self._response_queue_name)

        LOGGER.debug("rpc_call blocking waiting for response")

        response.blocker.wait(timeout=2.0)
        if self._pending_requests.get(corr_id):
            LOGGER.info("rpc_call timed out waiting for a response")
            self._pending_requests.pop(corr_id)
        else:
            LOGGER.info("rpc_call got response: {}".format(response))

        return response.response

    def rpc_cast(self, receiver, message, callback):
        """
        NOTE! Must enable_rpc_client before making calls to this function.

        Make an asynchronous call to an RPC server.

        :param receiver: name of the RPC server to send the request to
        :param message: message to send to the RPC server
        :param callback: callback for when response is gotten
        """
        raise NotImplementedError

    def handle_rpc_request(self, message: ConsumedMessage):
        """
        Handler for an incoming RPC request.

        :param message: consumed RPC request
        """
        LOGGER.debug("handle_rpc_request request: {}".format(message))

        answer = self._rpc_request_callback(message.message)

        self._producer.rpc_response(message.reply_to,
                                    answer,
                                    message.correlation_id)

    def handle_rpc_response(self, message: ConsumedMessage):
        """
        Handler for an incoming RPC response.

        :param message: consumed RPC response
        """
        LOGGER.debug(f"handle_rpc_response response: {message}")

        response: RPCResponse = \
            self._pending_requests.pop(message.correlation_id)
        response.response = message.message
        response.blocker.set()
