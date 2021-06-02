import threading

import time
import unittest

from datetime import datetime

from rabbitmq_client import RMQConsumer


def started(consumer, timeout=2.0):
    start_time = datetime.now()
    while not consumer.ready:
        elapsed_time = datetime.now() - start_time
        if elapsed_time.total_seconds() >= timeout:
            return False

        time.sleep(0.05)  # Wanne go fast

    return True


class IntegrationTestConsumer(unittest.TestCase):

    def setUp(self) -> None:
        self.consumer = RMQConsumer()
        self.consumer.start()
        self.assertTrue(started(self.consumer))

    def tearDown(self) -> None:
        self.consumer.stop()

    def test_stop_consumer(self):
        """
        Verify RMQConsumer, when stopped, shuts down completely and releases
        allocated resources.
        """
        # Run test
        self.consumer.stop()

        thread_count = len(threading.enumerate())
        # Only MainThread should still be running.
        self.assertEqual(1, thread_count)
