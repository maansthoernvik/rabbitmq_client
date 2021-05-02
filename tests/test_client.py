import time
import random
import unittest
import threading

from rabbitmq_client.errors import ConsumerAlreadyExists

from rabbitmq_client.client import RMQClient
from rabbitmq_client import rpc


"""
DEFINITIONS
"""

# ----------------------------------------------------------------------------
# RPC definitions
# ----------------------------------------------------------------------------
RPC_SERVER_NAME = "rpc_test_server"


def rpc_request_handler(message):
    return "answer".encode('utf-8')


def wait_until_rpc_ready(test, client, rpc_type, timeout=2.0):
    time_waited = 0.0

    if rpc_type == "server":
        while not client.is_rpc_server_ready():
            if time_waited > timeout:
                test.fail("RPC server not ready in time")

            time.sleep(0.1)
            time_waited += 0.1

    elif rpc_type == "client":
        while not client.is_rpc_client_ready():
            if time_waited > timeout:
                test.fail("RPC client not ready in time")

            time.sleep(0.1)
            time_waited += 0.1


# ----------------------------------------------------------------------------
# Subscription definitions
# ----------------------------------------------------------------------------
TEST_TOPIC_1 = "topic1"
TEST_TOPIC_2 = "topic2"
TEST_TOPIC_3 = "topic3"
TEST_TOPIC_4 = "topic4"


def wait_until_subscribed(test, client, topic, timeout=2.0):
    time_waited = 0.0

    while not client.is_subscribed(topic):
        if time_waited > timeout:
            test.fail("Did not subscribe in time")

        time.sleep(0.1)
        time_waited += 0.1


# ----------------------------------------------------------------------------
# Command queue definitions
# ----------------------------------------------------------------------------
COMMAND_QUEUE = "command_queue_name"


"""
Test cases
"""


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

    def tearDown(self):
        """
        Stops the client to release allocated resources at the end of each
        test.
        """
        self.client.stop()

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
        number_of_messages_per_topic = 100

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

    def test_publish_to_other_topic_than_subscription(self):
        """
        Tests publishing to a different topic than what is subscribed to,
        assuming that the message will not be received by the consumer.
        """
        gotten_messages = 0
        event = threading.Event()

        def subscription_callback_1(message):
            nonlocal gotten_messages

            gotten_messages += 1

        self.client.subscribe(TEST_TOPIC_1, subscription_callback_1)
        wait_until_subscribed(self, self.client, TEST_TOPIC_1)

        self.client.publish(TEST_TOPIC_1, b'message')
        self.client.publish(TEST_TOPIC_2, b'message')
        self.client.publish(TEST_TOPIC_3, b'message')
        self.client.publish(TEST_TOPIC_4, b'message')

        event.wait(timeout=1)

        self.assertEqual(gotten_messages, 1)

    def test_subscribe_to_same_topic_twice(self):
        """Verify that a topic cannot be subscribed to twice."""

        def subscription_callback(message):
            pass

        self.client.subscribe(TEST_TOPIC_1, subscription_callback)
        wait_until_subscribed(self, self.client, TEST_TOPIC_1)

        try:
            self.client.subscribe(TEST_TOPIC_1, subscription_callback)

            self.fail("Two subscriptions to the same topic should not be \
                      possible")
        except ConsumerAlreadyExists:
            pass  # the test case


class TestRPC(unittest.TestCase):
    """
    Tests for the client interface for RPC.
    """

    def setUp(self):
        """
        Initializes the client for each test case.
        """
        self.client = RMQClient()
        self.client.start()

    def test_rpc(self):
        """
        Tests the functionality of the RPC server and client.
        """
        self.client.enable_rpc_server(RPC_SERVER_NAME, rpc_request_handler)
        self.client.enable_rpc_client()
        wait_until_rpc_ready(self, self.client, "server")
        wait_until_rpc_ready(self, self.client, "client")

        response = self.client.rpc_call(RPC_SERVER_NAME, b'message')

        self.assertEqual(b'answer', response)

    def test_rpc_call_non_existent_server(self):
        """
        Tests issuing a call to a non-existent server
        """
        self.client.enable_rpc_server(RPC_SERVER_NAME, rpc_request_handler)
        self.client.enable_rpc_client()
        wait_until_rpc_ready(self, self.client, "server")
        wait_until_rpc_ready(self, self.client, "client")

        response = self.client.rpc_call("BULLSHIT", b'message')

        self.assertEqual(response, rpc.RPC_DEFAULT_REPLY)

        # test one request to the correct queue name as well to see nothing is
        # broken.
        response = self.client.rpc_call(RPC_SERVER_NAME, b'message')

        self.assertEqual(response, b'answer')

    def tearDown(self):
        """
        Stops the client to release allocated resources at the end of each
        test.
        """
        self.client.stop()


class TestCommand(unittest.TestCase):
    """Verify that Command queues and Commands work as intended."""

    def setUp(self) -> None:
        """
        Initializes the client for each test case.
        """
        self.client = RMQClient()
        self.client.start()

    def test_command_queue_basic(self):
        """
        Verify that a command queue can be declared and that the callback
        works.
        """

        event = threading.Event()
        gotten_command = None

        def command_callback(command):
            event.set()

            nonlocal gotten_command
            gotten_command = command

        self.client.command_queue(COMMAND_QUEUE, command_callback)
        self.client.command(COMMAND_QUEUE, b'command')

        event.wait(timeout=2)

        self.assertEqual(gotten_command, b'command')

    def tearDown(self):
        """
        Stops the client to release allocated resources at the end of each
        test.
        """
        self.client.stop()


class TestShutdown(unittest.TestCase):
    """Test that the client shuts down correctly and cleans up."""

    def test_shutdown_correct_thread_count(self):
        """Verifies the correct thread count after client stop."""
        client = RMQClient()
        client.start()

        # At the time of writing, this starts the QueueFeederThread for the
        # Consumer _work_queue, ensuring a correct thread count reading.
        client.enable_rpc_server(RPC_SERVER_NAME, rpc_request_handler)
        wait_until_rpc_ready(self, client, "server")

        ###################################
        # PRODUCER WORK THREAD FORCE START
        ###################################
        event = threading.Event()

        publish_received = False

        def subscription_callback(message):
            nonlocal publish_received
            publish_received = True
            event.set()

        client.subscribe(TEST_TOPIC_1, subscription_callback)
        wait_until_subscribed(self, client, TEST_TOPIC_1)

        client.publish(TEST_TOPIC_1, b'msg')

        event.wait(timeout=2)  # Confirms publish happens.
        self.assertEqual(True, publish_received)
        ###################################

        self.assertEqual(4, threading.active_count())

        client.stop()

        # Only main thread still lives, all other threads have been waited for,
        # ensuring synchronous termination.
        self.assertEqual(1, threading.active_count())

    def test_shutdown_multiple_clients(self):
        """
        Verify that multiple clients can be started and stopped individually.
        Resources are not shared between them. Clients shall be threadsafe.

        ORIGIN: log module shared between clients, causing shutdown problems.
        """
        client_1 = RMQClient()
        client_1.start()

        client_1.enable_rpc_server(RPC_SERVER_NAME + "1", rpc_request_handler)
        wait_until_rpc_ready(self, client_1, "server")

        # Expected thread count from one client
        # 1. Main thread
        # 2. Consumer monitor
        # 3. Consumer work queue (from starting RPC server)
        self.assertEqual(3, threading.active_count())

        # Start client 2
        client_2 = RMQClient()
        client_2.start()

        client_2.enable_rpc_server(RPC_SERVER_NAME + "2", rpc_request_handler)
        client_2.enable_rpc_client()
        wait_until_rpc_ready(self, client_2, "server")
        wait_until_rpc_ready(self, client_2, "client")

        # Expected thread count from two clients
        # + Consumer monitor
        # + Consumer work queue (from starting RPC server)
        self.assertEqual(5, threading.active_count())

        # Produce 1 item to increase thread count
        client_2.rpc_call(RPC_SERVER_NAME + "1", b'well hello there')

        # Expected thread count after producing a message
        # + Client 1 Producer work queue (RPC response)
        # + Client 2 Producer work queue
        self.assertEqual(7, threading.active_count())

        # Stop the first client
        client_1.stop()
        # print("Remaining threads after client_1 was stopped:")
        # for thread in threading.enumerate():
        #     print(thread)

        # Expected thread count after stopping the first client, who did not
        # produce anything.
        # - Consumer monitor
        # - Consumer work queue
        # - Producer work queue
        self.assertEqual(4, threading.active_count())

        # Stop the second client
        client_2.stop()
        # print("Remaining threads after client_2 was also stopped:")
        # for thread in threading.enumerate():
        #     print(thread)

        # Expected thread count after stopping both clients
        # - Consumer monitor
        # - Consumer work thread
        # - Producer work thread
        # ONLY MAIN THREAD SHALL REMAIN!
        self.assertEqual(1, threading.active_count())


if __name__ == '__main__':
    unittest.main()
