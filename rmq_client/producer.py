from .connection import RMQConnection
from multiprocessing import Process


class RMQProducer:
    _process: Process

    def __init__(self):
        pass

    def start(self):
        connection = RMQConnection()

        self._process = Process(target=connection.connect)
        self._process.start()
