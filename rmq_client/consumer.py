from .connection import RMQConnection


class RMQConsumer:
    _connection: RMQConnection

    def __init__(self):
        self._connection = RMQConnection()

    def start(self):
        self._connection.connect()

    def stop(self):
        self._connection.terminate()
