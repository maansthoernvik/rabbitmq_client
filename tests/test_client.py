import time
import unittest
import threading

from rmq_client.client import RMQClient


def rpc_request_handler(message):
    return "answer".encode('utf-8')


TEST_TOPIC_1 = "topic1"
TEST_TOPIC_2 = "topic2"
RPC_SERVER_NAME = "rpc_server"


class TestClient(unittest.TestCase):
    """
    Tests for the client interface
    """

    def setUp(self) -> None:
        """
        Initializes the client for each test case.
        """
        self.client = RMQClient()
        self.client.start()

    def test_single_subscription(self):
        """
        Verifies a single subscription and publishing for said subscription.
        """
        expected_message = b'message'
        gotten_message = b''

        event = threading.Event()

        def subscription_callback(message):
            event.set()

            nonlocal gotten_message
            gotten_message = message

        self.client.subscribe(TEST_TOPIC_1, subscription_callback)

        while not self.client.is_subscribed(TEST_TOPIC_1):
            time.sleep(0.1)

        self.client.publish(TEST_TOPIC_1, expected_message)

        event.wait(timeout=5)

        self.assertEqual(expected_message, gotten_message)

    def tearDown(self) -> None:
        """
        Stops the client to release allocated resources at the end of each test.
        """
        self.client.stop()


if __name__ == '__main__':
    unittest.main()
