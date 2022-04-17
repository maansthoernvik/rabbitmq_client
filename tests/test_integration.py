import threading
import time
import unittest
import warnings

import pika

from datetime import datetime
from threading import Event

from pika.exchange_type import ExchangeType

from rabbitmq_client import (
    RMQConsumer,
    RMQProducer,
    ConsumeParams,
    QueueParams,
    ExchangeParams,
    ConsumeOK,
    ConfirmModeOK
)


def started(rmq_client, timeout=2.0):
    start_time = datetime.now()
    while not rmq_client.ready:
        elapsed_time = datetime.now() - start_time
        if elapsed_time.total_seconds() >= timeout:
            return False

        time.sleep(0.05)  # Wanne go fast

    return True


class TestAcknowledgements(unittest.TestCase):

    def setUp(self) -> None:
        self.consumer = RMQConsumer()
        self.consumer.start()
        self.assertTrue(started(self.consumer))

        self.producer = RMQProducer()
        self.producer.start()
        self.assertTrue(started(self.producer))

        warnings.simplefilter("ignore", ResourceWarning)

    def tearDown(self) -> None:
        self.consumer.stop()
        self.producer.stop()

    @classmethod
    def tearDownClass(cls) -> None:
        """
        Need general teardown since not wanting to introduce waits part of
        test cases. Need to purge queues as consumer/producers are stopped
        before acking messages, sometimes.

        This teardown enabled re-testability without getting messages sent by
        a previous run.
        """
        client = RMQConsumer()
        client.start()
        assert started(client)

        purge_confirmation = threading.Event()
        purges = 0

        def purge_confirmed(*args, **kwargs):
            nonlocal purges
            purges += 1

            if purges == 3:
                purge_confirmation.set()

        client._channel.queue_purge("queue_man_ack",
                                    callback=purge_confirmed)
        client._channel.queue_purge("queue_man_ack_no_ack",
                                    callback=purge_confirmed)
        client._channel.queue_purge("queue_auto_ack",
                                    callback=purge_confirmed)

        purge_confirmation.wait(timeout=2.0)
        client.stop()

    def test_manual_ack_mode(self):
        consume_ok = threading.Event()
        msg = threading.Event()

        def on_msg(_body, ack=None):  # noqa
            if isinstance(_body, ConsumeOK):
                consume_ok.set()
            else:
                msg.set()
                ack()

        self.consumer.consume(
            ConsumeParams(on_msg),
            queue_params=QueueParams("queue_man_ack")
        )
        self.assertTrue(consume_ok.wait(timeout=2.0))  # await consume OK

        self.producer.publish(b"test_manual_ack_mode",
                              queue_params=QueueParams("queue_man_ack"))
        self.assertTrue(msg.wait(timeout=2.0))  # await msg

        # ack has been done, consumed messages should not be re-sent
        msg.clear()
        self.consumer.restart()
        self.assertFalse(msg.wait(timeout=0.5))

    def test_manual_ack_mode_no_manual_ack(self):
        consume_ok = threading.Event()
        msg = threading.Event()

        def on_msg(_body, ack=None):  # noqa
            if isinstance(_body, ConsumeOK):
                consume_ok.set()
            else:
                msg.set()

        self.consumer.consume(
            ConsumeParams(on_msg),
            queue_params=QueueParams("queue_man_ack_no_ack")
        )
        self.assertTrue(consume_ok.wait(timeout=2.0))  # await consume OK

        self.producer.publish(b"test_manual_ack_mode_no_manual_ack",
                              queue_params=QueueParams("queue_man_ack_no_ack"))
        self.assertTrue(msg.wait(timeout=2.0))  # await msg

        # no ack happens, restart the connection and see the message pop again
        msg.clear()
        self.consumer.restart()
        self.assertTrue(msg.wait(timeout=1.0))

    def test_auto_ack_mode(self):
        consume_ok = threading.Event()
        message_received = threading.Event()

        def on_msg(_body):  # noqa
            if isinstance(_body, ConsumeOK):
                consume_ok.set()
            else:
                message_received.set()

        self.consumer.consume(
            ConsumeParams(on_msg, auto_ack=True),
            queue_params=QueueParams("queue_auto_ack")
        )
        self.assertTrue(consume_ok.wait(timeout=2.0))  # await consume OK
        self.producer.publish(b"test_auto_ack_mode",
                              queue_params=QueueParams("queue_auto_ack"))
        self.assertTrue(message_received.wait(timeout=2.0))  # await msg

        # no ack happens, restart the connection and see the message pop again
        consume_ok.clear()
        message_received.clear()
        self.consumer.restart()
        self.assertTrue(consume_ok.wait(timeout=1.0))
        self.assertFalse(message_received.wait(timeout=0.5))


# noinspection DuplicatedCode
class TestIntegration(unittest.TestCase):

    # @classmethod
    # def setUpClass(cls) -> None:
    #     logger = logging.getLogger("rabbitmq_client")
    #     logger.addHandler(logging.StreamHandler())

    def setUp(self) -> None:
        self.consumer = RMQConsumer(
            connection_parameters=pika.ConnectionParameters())
        self.consumer.start()
        self.assertTrue(started(self.consumer))

        self.producer = RMQProducer()
        self.producer.start()
        self.assertTrue(started(self.producer))

        warnings.simplefilter("ignore", ResourceWarning)

    def tearDown(self) -> None:
        self.consumer.stop()
        self.producer.stop()

    @classmethod
    def tearDownClass(cls) -> None:
        """
        Need general teardown since not wanting to introduce waits part of
        test cases. Need to purge queues as consumer/producers are stopped
        before acking messages, sometimes.

        This teardown enabled re-testability without getting messages sent by
        a previous run.
        """
        client = RMQConsumer()
        client.start()
        assert started(client)

        purge_confirmation = threading.Event()
        purges = 0

        def purge_confirmed(*args, **kwargs):
            nonlocal purges
            purges += 1

            if purges == 3:
                purge_confirmation.set()

        client._channel.queue_purge("queue_fanout_receiver",
                                    callback=purge_confirmed)
        client._channel.queue_purge("queue_direct_receiver",
                                    callback=purge_confirmed)
        client._channel.queue_purge("queue",
                                    callback=purge_confirmed)

        purge_confirmation.wait(timeout=2.0)
        client.stop()

    def test_stop_consumer(self):
        """
        Verify RMQConsumer, when stopped, shuts down completely and releases
        allocated resources.
        """
        # Run test
        self.consumer.stop()

        thread_count = len(threading.enumerate())

        # Only MainThread should still be running.
        self.assertEqual(2, thread_count)

    def test_stop_producer(self):
        """
        Verify RMQProducer, when stopped, shuts down completely and releases
        allocated resources.
        """
        # Run test
        self.producer.stop()

        thread_count = len(threading.enumerate())

        # Only MainThread should still be running.
        self.assertEqual(2, thread_count)

    def test_consume_from_queue(self):
        """Verify RMQConsumer can consume from a queue."""
        msg_received = None
        event = threading.Event()

        def on_msg(msg):
            event.set()

            nonlocal msg_received
            msg_received = msg

        # Consume
        consume_key = self.consumer.consume(ConsumeParams(on_msg,
                                                          auto_ack=True),
                                            queue_params=QueueParams("queue"))
        self.assertEqual(consume_key, "queue")

        # Wait for ConsumeOK
        event.wait(timeout=2.0)
        self.assertTrue(isinstance(msg_received, ConsumeOK))

        # Send a message and verify it is delivered OK
        event.clear()  # clear to re-use event
        self.producer.publish(b"test_consume_from_queue",
                              queue_params=QueueParams("queue"))

        event.wait(timeout=1.0)
        self.assertEqual(msg_received, b"test_consume_from_queue")

    def test_consume_from_direct_exchange(self):
        """
        Verify RMQConsumer can consume from a direct exchange and routing key.
        """
        msg_received = None
        event = threading.Event()

        def on_msg(msg):
            event.set()

            nonlocal msg_received
            msg_received = msg

        # Consume
        consume_key = self.consumer.consume(
            ConsumeParams(on_msg, auto_ack=True),
            exchange_params=ExchangeParams("exchange"),
            routing_key="bla.bla"
        )
        self.assertEqual(consume_key, "exchange|bla.bla")

        # Wait for ConsumeOK
        event.wait(timeout=1.0)
        self.assertTrue(isinstance(msg_received, ConsumeOK))

        # Send a message and verify it is delivered OK
        event.clear()
        self.producer.publish(b"test_consume_from_direct_exchange",
                              exchange_params=ExchangeParams("exchange"),
                              routing_key="bla.bla")

        event.wait(timeout=1.0)
        self.assertEqual(msg_received, b"test_consume_from_direct_exchange")

    def test_consume_from_fanout_exchange(self):
        """
        Verify RMQConsumer can consume from a fanout exchange.
        """
        msg_received = None
        event = threading.Event()

        def on_msg(msg):
            event.set()

            nonlocal msg_received
            msg_received = msg

        # Consume
        consume_key = self.consumer.consume(
            ConsumeParams(on_msg, auto_ack=True),
            exchange_params=ExchangeParams("exchange_fanout",
                                           exchange_type=ExchangeType.fanout)
        )
        self.assertEqual(consume_key, "exchange_fanout")

        # Wait for ConsumeOK
        event.wait(timeout=1.0)
        self.assertTrue(isinstance(msg_received, ConsumeOK))

        # Send a message and verify it is delivered OK
        event.clear()
        self.producer.publish(
            b"test_consume_from_fanout_exchange",
            exchange_params=ExchangeParams("exchange_fanout",
                                           exchange_type=ExchangeType.fanout))

        event.wait(timeout=1.0)
        self.assertEqual(msg_received, b"test_consume_from_fanout_exchange")

    def test_consume_from_exchange_and_queue(self):
        """
        Verify RMQConsumer can consume from an exchange, binding it to a
        specific queue.
        """
        msg_received = None
        event = threading.Event()

        def on_msg(msg):
            event.set()

            nonlocal msg_received
            msg_received = msg

        # Consume
        consume_key = self.consumer.consume(
            ConsumeParams(on_msg, auto_ack=True),
            exchange_params=ExchangeParams("exchange_fanout",
                                           exchange_type=ExchangeType.fanout),
            queue_params=QueueParams("queue_fanout_receiver")
        )
        self.assertEqual(consume_key, "queue_fanout_receiver|exchange_fanout")

        # Wait for ConsumeOK
        event.wait(timeout=1.0)
        self.assertTrue(isinstance(msg_received, ConsumeOK))

        # Send a message and verify it is delivered OK
        event.clear()
        self.producer.publish(
            b"test_consume_from_exchange_and_queue",
            exchange_params=ExchangeParams("exchange_fanout",
                                           exchange_type=ExchangeType.fanout))

        event.wait(timeout=1.0)
        self.assertEqual(msg_received, b"test_consume_from_exchange_and_queue")

    def test_consume_from_exchange_routing_key_and_queue(self):
        """
        Verify RMQConsumer can consume from an exchange, binding it to a
        specific queue.
        """
        msg_received = None
        event = threading.Event()

        def on_msg(msg):
            event.set()

            nonlocal msg_received
            msg_received = msg

        # Consume
        consume_key = self.consumer.consume(
            ConsumeParams(on_msg, auto_ack=True),
            exchange_params=ExchangeParams("exchange_direct"),
            routing_key="fish",
            queue_params=QueueParams("queue_direct_receiver")
        )
        self.assertEqual(consume_key,
                         "queue_direct_receiver|exchange_direct|fish")

        # Wait for ConsumeOK
        event.wait(timeout=1.0)
        self.assertTrue(isinstance(msg_received, ConsumeOK))

        # Send a message and verify it is delivered OK
        event.clear()
        self.producer.publish(
            b"test_consume_from_exchange_routing_key_and_queue",
            exchange_params=ExchangeParams("exchange_direct"),
            routing_key="fish"
        )

        event.wait(timeout=1.0)
        self.assertEqual(msg_received,
                         b"test_consume_from_exchange_routing_key_and_queue")


class TestConfirmMode(unittest.TestCase):

    def setUp(self) -> None:
        self.consumer = RMQConsumer()
        self.consumer.start()
        self.assertTrue(started(self.consumer))

        self.producer = RMQProducer()
        self.producer.start()
        self.assertTrue(started(self.producer))

        warnings.simplefilter("ignore", ResourceWarning)

    def tearDown(self) -> None:
        self.consumer.stop()
        self.producer.stop()

    @classmethod
    def tearDownClass(cls) -> None:
        """
        Need general teardown since not wanting to introduce waits part of
        test cases. Need to purge queues as consumer/producers are stopped
        before acking messages, sometimes.

        This teardown enabled re-testability without getting messages sent by
        a previous run.
        """
        client = RMQConsumer()
        client.start()
        assert started(client)

        purge_confirmation = threading.Event()
        purges = 0

        def purge_confirmed(*args, **kwargs):
            nonlocal purges
            purges += 1

            if purges == 2:
                purge_confirmation.set()

        client._channel.queue_purge("queue_fanout_receiver",
                                    callback=purge_confirmed)
        client._channel.queue_purge("queue",
                                    callback=purge_confirmed)

        purge_confirmation.wait(timeout=2.0)
        client.stop()

    def test_confirm_mode(self):
        """
        Verify confirm mode pushes publish_keys to users on successful
        delivery.
        """
        msg_received = None
        event = threading.Event()

        def on_confirm(confirm):
            event.set()

            nonlocal msg_received
            msg_received = confirm

        # Activate confirm mode
        self.producer.activate_confirm_mode(on_confirm)

        # Wait for ConfirmModeOK
        event.wait(timeout=1.0)
        self.assertTrue(isinstance(msg_received, ConfirmModeOK))

        # Send a message and verify it is delivered OK
        event.clear()
        publish_key = self.producer.publish(
            b"body",
            exchange_params=ExchangeParams("exchange_direct"),
        )

        event.wait(timeout=1.0)
        self.assertEqual(msg_received, publish_key)
        self.assertEqual(len(self.producer._unacked_publishes.keys()), 0)

    def test_buffer_publishes_with_confirm_mode_on(self):
        """
        Verify buffering publishes, with confirm mode on, and then starting the
        producer results in a correct number of messages being sent
        successfully, measured by counting and verifying the Basic.Acks
        received from the broker.
        """
        producer = RMQProducer()
        event = threading.Event()
        key_dict = dict()

        def on_confirm(confirm):
            if not isinstance(confirm, ConfirmModeOK):
                key_dict.pop(confirm)

            if len(key_dict.keys()) == 0:
                event.set()

        # Activate confirm mode
        producer.activate_confirm_mode(on_confirm)

        # Buffer a few publishes
        key_dict[
            producer.publish(
                b"body", exchange_params=ExchangeParams("exchange_direct"))
        ] = False
        key_dict[
            producer.publish(
                b"body", exchange_params=ExchangeParams("exchange_direct"))
        ] = False
        key_dict[
            producer.publish(
                b"body", exchange_params=ExchangeParams("exchange_direct"))
        ] = False
        self.assertEqual(len(producer._unacked_publishes.keys()), 0)
        self.assertEqual(len(producer._buffered_messages), 3)

        # Start the producer
        producer.start()
        self.assertTrue(started(producer))

        # Await all confirms
        event.wait(timeout=1.0)
        self.assertEqual(len(producer._unacked_publishes.keys()), 0)

        # Stop the extra producer
        producer.stop()


class TestCaching(unittest.TestCase):

    def setUp(self) -> None:
        # logger = logging.getLogger("rabbitmq_client")
        # logger.setLevel(logging.DEBUG)
        # stream_handler = logging.StreamHandler()
        # stream_handler.setLevel(logging.DEBUG)
        # logger.addHandler(stream_handler)

        self.consumer = RMQConsumer()
        self.consumer.start()
        self.assertTrue(started(self.consumer))

        self.producer = RMQProducer()
        self.producer.start()
        self.assertTrue(started(self.producer))

        warnings.simplefilter("ignore", ResourceWarning)

    def tearDown(self) -> None:
        self.consumer.stop()
        self.producer.stop()

    @classmethod
    def tearDownClass(cls) -> None:
        """
        Need general teardown since not wanting to introduce waits part of
        test cases. Need to purge queues as consumer/producers are stopped
        before acking messages, sometimes.

        This teardown enabled re-testability without getting messages sent by
        a previous run.
        """
        client = RMQConsumer()
        client.start()
        assert started(client)

        purge_confirmation = threading.Event()

        def purge_confirmed(*args, **kwargs):
            purge_confirmation.set()

        client._channel.queue_purge("cached_queue",
                                    callback=purge_confirmed)

        purge_confirmation.wait(timeout=2.0)
        client.stop()

    def test_caching_queue(self):
        consume_ok = Event()
        msg_gotten = Event()
        msg_content = ""

        def on_queue_message(msg, ack=None):
            if isinstance(msg, ConsumeOK):
                consume_ok.set()

                return

            nonlocal msg_content
            msg_content = msg
            msg_gotten.set()
            ack()

        consume_params = ConsumeParams(on_queue_message)
        queue_params = QueueParams("cached_queue")
        exchange_params = ExchangeParams("cached_exchange")

        self.consumer.consume(consume_params, queue_params=queue_params)
        self.assertTrue(consume_ok.wait(timeout=1.0))
        consume_ok.clear()

        # Publish #1
        self.producer.publish(b"cached_queue body", queue_params=queue_params)
        self.assertTrue(msg_gotten.wait(timeout=1.0))
        self.assertEqual(msg_content, b"cached_queue body")
        msg_gotten.clear()

        # Publish #2
        self.producer.publish(b"another cached_queue body",
                              queue_params=queue_params)
        self.assertTrue(msg_gotten.wait(timeout=1.0))
        self.assertEqual(msg_content, b"another cached_queue body")
        msg_gotten.clear()

        self.consumer.consume(
            consume_params,
            queue_params=queue_params,
            exchange_params=ExchangeParams("cached_exchange"),
            routing_key="routing_key"
        )
        self.assertTrue(consume_ok.wait(timeout=1.0))

        # Publish #3 to cached_exchange with routing key: routing_key
        self.producer.publish(b"cached_exchange+routing_key body",
                              exchange_params=exchange_params,
                              routing_key="routing_key")
        self.assertTrue(msg_gotten.wait(timeout=1.0))
        self.assertEqual(msg_content, b"cached_exchange+routing_key body")
        msg_gotten.clear()

        # Publish #4 to cached_exchange with routing key: routing_key
        self.producer.publish(b"cached_exchange+routing_key body",
                              exchange_params=exchange_params,
                              routing_key="routing_key")
        self.assertTrue(msg_gotten.wait(timeout=1.0))
        self.assertEqual(msg_content, b"cached_exchange+routing_key body")
        msg_gotten.clear()
