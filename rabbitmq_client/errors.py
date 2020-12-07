"""
Consumer errors
"""


class ConsumerAlreadyExists(Exception):
    pass


"""
RPC errors
"""


class RPCClientNotReady(Exception):
    pass
