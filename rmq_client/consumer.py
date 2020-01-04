from multiprocessing import Queue as IPCQueue, Process
from threading import Thread

from .consumer_connection import create_consumer_connection


class RMQConsumer:

    # Pub/sub
    _topic_callbacks: dict

    # IPC
    _connection_process: Process

    _work_queue: IPCQueue
    _consumed_messages: IPCQueue

    def __init__(self):
        self._topic_callbacks = dict()
        
        self._work_queue = IPCQueue()
        self._consumed_messages = IPCQueue()

    def start(self):
        self._connection_process = Process(
            target=create_consumer_connection,
            args=(self._work_queue, self._consumed_messages)
        )
        self._connection_process.start()

        thread = Thread(target=self.consume, daemon=True)
        thread.start()

    def consume(self):
        message = self._consumed_messages.get()
        self.handle_message(message)
        self.consume()

    def handle_message(self, message):
        print(message)

    def stop(self):
        self._connection_process.terminate()

    def subscribe(self, topic, routing_key, callback):
        # 1. Add topic + routing_key to list of topics to consume on
        # 2. Add callback to be called when event on that topic + routing_key
        pass
