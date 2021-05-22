import unittest

from unittest.mock import patch, Mock

from rabbitmq_client import ExchangeParams, ConsumeParams, QueueParams
from rabbitmq_client.new_consumer import RMQConsumer, _gen_consume_key


class TestConsumeKeyGeneration(unittest.TestCase):

    def test_consume_key_gen(self):
        """Verify the consume key gen works as intended"""
        self.assertEqual(
            "queue|exchange|routing_key",
            _gen_consume_key(
                QueueParams("queue"), ExchangeParams("exchange"), "routing_key"
            )
        )
        self.assertEqual(
            "exchange|routing_key",
            _gen_consume_key(
                None, ExchangeParams("exchange"), "routing_key"
            )
        )
        self.assertEqual(
            "queue|routing_key",
            _gen_consume_key(
                QueueParams("queue"), None, "routing_key"
            )
        )
        self.assertEqual(
            "queue|exchange|complex.routing.key",
            _gen_consume_key(
                QueueParams("queue"),
                ExchangeParams("exchange"),
                "complex.routing.key"
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
        self.consumer.consume(consume, exchange_params=exchange)

        # A single consume should exist, that is not confirmed
        consume_key = _gen_consume_key(exchange_params=exchange)
        consume_instance = self.consumer._consumes[consume_key]

        # Assertions
        self.assertEqual(consume_instance.consume_params, consume)
        self.assertEqual(consume_instance.exchange_params, exchange)

    def test_consume_queue_only(self):
        """Verify possibility to consume from a queue."""
        def on_msg(): pass
        consume = ConsumeParams(
            on_msg
        )
        queue = QueueParams(
            "queue"
        )
        self.consumer.consume(consume, queue_params=queue)

        # A single consume should exist, that is not confirmed
        consume_key = _gen_consume_key(queue_params=queue)
        consume_instance = self.consumer._consumes[consume_key]

        # Assertions
        self.assertEqual(consume_instance.consume_params, consume)
        self.assertEqual(consume_instance.queue_params, queue)

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
        self.consumer.consume(
            consume, queue_params=queue, exchange_params=exchange
        )

        # A single consume should exist, that is not confirmed
        consume_key = _gen_consume_key(
            queue_params=queue, exchange_params=exchange
        )
        consume_instance = self.consumer._consumes[consume_key]

        # Assertions
        self.assertEqual(consume_instance.consume_params, consume)
        self.assertEqual(consume_instance.queue_params, queue)
        self.assertEqual(consume_instance.exchange_params, exchange)

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
            consume,
            queue_params=queue,
            exchange_params=exchange,
            routing_key="routing_key"
        )

        # A single consume should exist, that is not confirmed
        consume_key = _gen_consume_key(
            queue_params=queue,
            exchange_params=exchange,
            routing_key="routing_key"
        )
        consume_instance = self.consumer._consumes[consume_key]

        # Assertions
        self.assertEqual(consume_instance.consume_params, consume)
        self.assertEqual(consume_instance.queue_params, queue)
        self.assertEqual(consume_instance.exchange_params, exchange)
        self.assertEqual(consume_instance.routing_key, "routing_key")

    def test_consume_neither_queue_nor_exchange_provided(self):
        """
        Verify that consume raises an exception if neither queue nor exchange
        is provided.
        """
        with self.assertRaises(ValueError):
            self.consumer.consume(ConsumeParams(lambda _: ...))

    def test_consume_same_consume_key(self):
        """
        Verify that calling consume once more with the same queue name,
        exchange name and routing key will result in an exception.
        """
        self.consumer.consume(
            ConsumeParams(lambda _: ...), QueueParams("queue")
        )

        # Assertions
        with self.assertRaises(ValueError):
            self.consumer.consume(
                ConsumeParams(lambda _: ...), QueueParams("queue")
            )

    def test_on_close_marks_consumes_stopped(self):
        """Verify a call to on_close marks all consumes as stopped."""
        queue_consume = ConsumeParams(lambda _: ...)
        queue = QueueParams("queue")
        self.consumer.consume(queue_consume, queue_params=queue)

        exchange_consume = ConsumeParams(lambda _: ...)
        exchange = ExchangeParams("exchange")
        self.consumer.consume(exchange_consume, exchange_params=exchange)

        # Fake having all consumes started
        for _, consume in self.consumer._consumes.items():
            consume.consumer_tag = "some tag"

        # Run test
        self.consumer.on_close()

        # Checks
        for _, consume in self.consumer._consumes.items():
            if consume.consumer_tag == "some tag":
                self.fail(
                    "a consume did not have its consumer tag cleared when "
                    "on_close was called"
                )

    def test_consume_when_ready_queue_only(self):
        """
        Verify work is sent to connection if consumer is ready when consume
        is called.
        """
        # Set up
        self.consumer.declare_queue = Mock()
        self.consumer.on_ready()  # Connection reports it is ready

        # Run test
        consume_params = ConsumeParams(lambda _: ...)
        queue_params = QueueParams("queue")
        self.consumer.consume(
            consume_params, queue_params=queue_params
        )

        # Assertions
        self.consumer.declare_queue.assert_called_with(
            queue_params, consume_params=consume_params
        )

    def test_consume_when_ready_exchange_only(self):
        """
        Verify work is sent to connection if consumer is ready when consume
        is called.
        """
        # Set up
        self.consumer.declare_exchange = Mock()
        self.consumer.on_ready()  # Connection reports it is ready

        # Run test
        consume_params = ConsumeParams(lambda _: ...)
        exchange_params = ExchangeParams("exchange")
        self.consumer.consume(
            consume_params, exchange_params=exchange_params
        )

        # Assertions
        self.consumer.declare_exchange.assert_called_with(
            exchange_params,
            queue_params=None,
            routing_key=None,
            consume_params=consume_params
        )

    def test_consume_when_ready_both_queue_and_exchange(self):
        """
        Verify work is sent to connection if consumer is ready when consume
        is called.
        """
        # Set up
        self.consumer.declare_exchange = Mock()
        self.consumer.on_ready()  # Connection reports it is ready

        # Run test
        consume_params = ConsumeParams(lambda _: ...)
        exchange_params = ExchangeParams("exchange")
        queue_params = QueueParams("queue")
        self.consumer.consume(
            consume_params,
            queue_params=queue_params,
            exchange_params=exchange_params
        )

        # Assertions
        self.consumer.declare_exchange.assert_called_with(
            exchange_params,
            queue_params=queue_params,
            routing_key=None,
            consume_params=consume_params
        )

    def test_consume_before_ready_does_nothing(self):
        """
        Verify that if consume is called before the consumer is ready, nothing
        happens.
        """
        # Set up
        self.consumer.declare_queue = Mock()

        # Run test
        self.consumer.consume(
            ConsumeParams(lambda _: ...), queue_params=QueueParams("queue")
        )

        # Assertions
        self.consumer.declare_queue.assert_not_called()

    def test_on_ready_starts_consumes(self):
        """
        Verify a call to on_ready will lead to all consumes being re-issued.
        """
        # Set up
        self.consumer.declare_queue = Mock()
        self.consumer.declare_exchange = Mock()

        queue_consume = ConsumeParams(lambda _: ...)
        queue_params = QueueParams("queue")
        self.consumer.consume(queue_consume, queue_params=queue_params)

        exchange_consume = ConsumeParams(lambda _: ...)
        exchange_params = ExchangeParams("exchange")
        self.consumer.consume(
            exchange_consume, exchange_params=exchange_params
        )

        # Run test
        self.consumer.on_ready()  # Now the connection is ready!

        # Assertions
        self.consumer.declare_queue.assert_called_with(
            queue_params, consume_params=queue_consume
        )
        self.consumer.declare_exchange.assert_called_with(
            exchange_params,
            queue_params=None,
            routing_key=None,
            consume_params=exchange_consume
        )
