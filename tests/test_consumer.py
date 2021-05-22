import unittest

from unittest.mock import patch

from rabbitmq_client import ExchangeParams, ConsumeParams, QueueParams
from rabbitmq_client.new_consumer import RMQConsumer, _gen_consume_key


def queue(name):
    return QueueParams(name)


def exchange(name):
    return ExchangeParams(name)


class TestConsumeKeyGeneration(unittest.TestCase):

    def test_consume_key_gen(self):
        """Verify the consume key gen works as intended"""
        self.assertEqual(
            "queue|exchange|routing_key",
            _gen_consume_key(
                queue("queue"), exchange("exchange"), "routing_key"
            )
        )
        self.assertEqual(
            "exchange|routing_key",
            _gen_consume_key(
                None, exchange("exchange"), "routing_key"
            )
        )
        self.assertEqual(
            "queue|routing_key",
            _gen_consume_key(
                queue("queue"), None, "routing_key"
            )
        )
        self.assertEqual(
            "queue|exchange|complex.routing.key",
            _gen_consume_key(
                queue("queue"), exchange("exchange"), "complex.routing.key"
            )
        )


class TestConsumer(unittest.TestCase):
    """
    Test the new (2021) RMQConsumer class, verify its interface methods can be
    used as advertised and in different combinations.
    """

    @patch("rabbitmq_client.new_consumer.RMQConnection.start")
    def setUp(self, _connection_start) -> None:
        """Setup to run before each test case."""
        self.consumer = RMQConsumer()
        self.consumer.start()

    def test_consume_exchange_only(self):
        """Verify possibility to consume from an exchange."""
        def on_msg(): pass
        consume = ConsumeParams(
            on_msg
        )
        exchange = ExchangeParams(
            "exchange"
        )
        self.consumer.consume(consume, exchange=exchange)

        # A single consume should exist, that is not confirmed
        consume_key = _gen_consume_key(exchange=exchange)
        consume_instance = self.consumer._running_consumes[consume_key]

        self.assertEqual(consume_instance.consume, consume)
        self.assertEqual(consume_instance.exchange, exchange)

    def test_consume_queue_only(self):
        """Verify possibility to consume from a queue."""
        def on_msg(): pass
        consume = ConsumeParams(
            on_msg
        )
        queue = QueueParams(
            "queue"
        )
        self.consumer.consume(consume, queue=queue)

        # A single consume should exist, that is not confirmed
        consume_key = _gen_consume_key(queue=queue)
        consume_instance = self.consumer._running_consumes[consume_key]

        self.assertEqual(consume_instance.consume, consume)
        self.assertEqual(consume_instance.queue, queue)

    def test_consume_both_queue_and_exchange(self):
        """
        Verify possibility to provide both an exchange and a queue to the
        consume method.
        """
        def on_msg(): pass
        consume = ConsumeParams(
            on_msg
        )
        queue = QueueParams(
            "queue"
        )
        exchange = ExchangeParams(
            "exchange"
        )
        self.consumer.consume(consume, queue=queue, exchange=exchange)

        # A single consume should exist, that is not confirmed
        consume_key = _gen_consume_key(queue=queue, exchange=exchange)
        consume_instance = self.consumer._running_consumes[consume_key]

        self.assertEqual(consume_instance.consume, consume)
        self.assertEqual(consume_instance.queue, queue)
        self.assertEqual(consume_instance.exchange, exchange)

    def test_consume_queue_exchange_and_routing_key(self):
        """
        Verify possibility to provide both an exchange, routing_key and a queue
        to the consume method.
        """
        def on_msg(): pass
        consume = ConsumeParams(
            on_msg
        )
        queue = QueueParams(
            "queue"
        )
        exchange = ExchangeParams(
            "exchange"
        )
        self.consumer.consume(
            consume, queue=queue, exchange=exchange, routing_key="routing_key"
        )

        # A single consume should exist, that is not confirmed
        consume_key = _gen_consume_key(
            queue=queue, exchange=exchange, routing_key="routing_key"
        )
        consume_instance = self.consumer._running_consumes[consume_key]

        self.assertEqual(consume_instance.consume, consume)
        self.assertEqual(consume_instance.queue, queue)
        self.assertEqual(consume_instance.exchange, exchange)
        self.assertEqual(consume_instance.routing_key, "routing_key")

    def test_neither_queue_nor_exchange_provided(self):
        """
        Verify that consume raises an exception if neither queue nor exchange
        is provided.
        """
        with self.assertRaises(ValueError):
            self.consumer.consume(ConsumeParams(lambda _: ...))

    def test_consume_same_consume_key_raises_exception(self):
        """
        Verify that calling consume once more with the same queue name,
        exchange name and routing key will result in an exception.
        """
        self.consumer.consume(
            ConsumeParams(lambda _: ...), QueueParams("queue")
        )
        with self.assertRaises(ValueError):
            self.consumer.consume(
                ConsumeParams(lambda _: ...), QueueParams("queue")
            )
