from rabbitmq_client.new_connection import RMQConnection


class RMQConsumer(RMQConnection):
    """
    Generic consumer implementation using the RMQConnection base class to ease
    connection and channel handling.
    """

    def __init__(self):
        """"""
        super().__init__()
        self.ready = False

    def on_ready(self):
        """Connection hook, called when channel opened."""
        self.ready = True

    def on_close(self):
        """Connection hook, called when channel or connection closed."""
        self.ready = False

    def on_error(self):
        """
        Connection hook, called when the connection has encountered an error.
        """
        pass

    def consume(self):
        """

        """
        # sanity check parameters
        # exchanges:
        # - deny setting a routing key for fanout exchanges
        #
        # queues
        #
