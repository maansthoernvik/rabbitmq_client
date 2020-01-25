import time
import random
import unittest
import threading

from rmq_client.client import RMQClient


def rpc_request_handler(message):
    return "answer".encode('utf-8')


def wait_until_subscribed(test, client, topic, timeout=2.0):
    time_waited = 0.0
    while not client.is_subscribed(topic):
        if time_waited > timeout:
            test.fail("Did not subscribe in time")

        time.sleep(0.1)
        time_waited += 0.1


TEST_TOPIC_1 = "topic1"
TEST_TOPIC_2 = "topic2"
TEST_TOPIC_3 = "topic3"
TEST_TOPIC_4 = "topic4"
RPC_SERVER_NAME = "rpc_server"


class TestSubscription(unittest.TestCase):
    """
    Tests for the client interface for subscriptions.
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

        wait_until_subscribed(self, self.client, TEST_TOPIC_1)

        self.client.publish(TEST_TOPIC_1, expected_message)

        event.wait(timeout=5)

        self.assertEqual(expected_message, gotten_message)

    def test_multiple_subscriptions(self):
        """
        Tests multiple subscriptions
        """
        topic_1_messages = []
        topic_2_messages = []
        number_of_messages_per_topic = 20

        def subscription_callback_1(message):
            topic_1_messages.append(message)

        def subscription_callback_2(message):
            topic_2_messages.append(message)

        self.client.subscribe(TEST_TOPIC_1, subscription_callback_1)
        self.client.subscribe(TEST_TOPIC_2, subscription_callback_2)

        wait_until_subscribed(self, self.client, TEST_TOPIC_1)
        wait_until_subscribed(self, self.client, TEST_TOPIC_2)

        i = 0
        while i < number_of_messages_per_topic:
            self.client.publish(TEST_TOPIC_1, b'message')
            self.client.publish(TEST_TOPIC_2, b'message')
            i += 1

        time_waited = 0.0
        timeout = 5.0
        while len(topic_1_messages) < number_of_messages_per_topic and \
                len(topic_2_messages) < number_of_messages_per_topic:
            if time_waited > timeout:
                self.fail("Took too long to receive messages")

            time.sleep(0.1)
            time_waited += 0.1

    def test_absolute_chaos(self):
        """
        Tests multiple messages being sent to multiple topics at the same time
        and in different orders.
        """
        random.seed()

        topic_1_messages = []
        topic_2_messages = []
        topic_3_messages = []
        topic_4_messages = []
        number_of_messages_per_topic = 20

        def subscription_callback_1(message):
            topic_1_messages.append(message)

        def subscription_callback_2(message):
            topic_2_messages.append(message)

        def subscription_callback_3(message):
            topic_3_messages.append(message)

        def subscription_callback_4(message):
            topic_4_messages.append(message)

        self.client.subscribe(TEST_TOPIC_1, subscription_callback_1)
        self.client.subscribe(TEST_TOPIC_2, subscription_callback_2)
        self.client.subscribe(TEST_TOPIC_3, subscription_callback_3)
        self.client.subscribe(TEST_TOPIC_4, subscription_callback_4)

        wait_until_subscribed(self, self.client, TEST_TOPIC_1)
        wait_until_subscribed(self, self.client, TEST_TOPIC_2)
        wait_until_subscribed(self, self.client, TEST_TOPIC_3)
        wait_until_subscribed(self, self.client, TEST_TOPIC_4)

        i = 0
        while i < (number_of_messages_per_topic * 4):
            random_topic = random.choice([
                TEST_TOPIC_1, TEST_TOPIC_2, TEST_TOPIC_3, TEST_TOPIC_4
            ])

            self.client.publish(random_topic, b'message')
            i += 1

        time_waited = 0.0
        timeout = 5.0
        while len(topic_1_messages +
                  topic_2_messages +
                  topic_3_messages +
                  topic_4_messages) < number_of_messages_per_topic * 4:
            if time_waited > timeout:
                self.fail("Took too long to receive messages")

            time.sleep(0.1)
            time_waited += 0.1

        # At least some message was sent to each topic... Could use some better
        # verification, but difficult to make stable due to the random nature
        # of the test.
        self.assertNotEqual(len(topic_1_messages), 0)
        self.assertNotEqual(len(topic_2_messages), 0)
        self.assertNotEqual(len(topic_3_messages), 0)
        self.assertNotEqual(len(topic_4_messages), 0)

    def tearDown(self) -> None:
        """
        Stops the client to release allocated resources at the end of each test.
        """
        self.client.stop()


if __name__ == '__main__':
    unittest.main()
