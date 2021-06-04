import unittest

from unittest.mock import Mock, patch, ANY

from rabbitmq_client import (
    RMQProducer,
    ExchangeParams,
    QueueParams,
    PublishParams
)


# noinspection DuplicatedCode
class TestProducer(unittest.TestCase):
    """
    Test the RMQProducer implementation, which implements the RMQConnection
    interface.
    """

    @patch("rabbitmq_client.producer.RMQProducer.start")
    def setUp(self, _connection_start) -> None:
        """Setup to run before each test case."""
        self.producer = RMQProducer()
        self.producer.start()
        self.producer.on_ready()  # Fake connection getting ready

        self.producer.declare_queue = Mock()
        self.producer.declare_exchange = Mock()
        self.producer.bind_queue = Mock()
        self.producer.basic_publish = Mock()

    def test_producer_readiness(self):
        """Verify the producer ready property changes appropriately."""
        self.assertTrue(self.producer.ready)
        self.producer.on_close()
        self.assertFalse(self.producer.ready)
        self.producer.on_ready()
        self.assertTrue(self.producer.ready)
        self.producer.on_close(permanent=True)
        self.assertFalse(self.producer.ready)

    def test_producer_error_handling(self):
        """Verify on_error results in correct behavior..."""
        with self.assertRaises(NotImplementedError):
            self.producer.on_error()

    def test_publish_using_exchange_params(self):
        """Verify possibility to publish to an exchange."""
        # Prep
        exchange_params = ExchangeParams("exchange")

        # Run test + assertions
        self.producer.publish(b"body", exchange_params=exchange_params)
        self.producer.declare_exchange.assert_called_with(
            exchange_params, callback=ANY
        )

        self.producer.on_exchange_declared(b"body",
                                           exchange_params,
                                           None,
                                           routing_key="",
                                           publish_params=None)
        self.producer.basic_publish.assert_called_with(
            b"body",
            exchange=exchange_params.exchange,
            routing_key="",
            publish_params=None
        )

    def test_publish_using_queue_params(self):
        """Verify possibility to publish providing only queue parameters."""
        # Prep
        queue_params = QueueParams("queue")
        frame_mock = Mock()
        frame_mock.method.queue = "queue"

        # Run test + assertions
        self.producer.publish(b"body", queue_params=queue_params)
        self.producer.declare_queue.assert_called_with(
            queue_params, callback=ANY
        )

        self.producer.on_queue_declared(b"body",
                                        frame_mock,
                                        publish_params=None)
        self.producer.basic_publish.assert_called_with(
            b"body",
            routing_key=queue_params.queue,
            publish_params=None
        )

    def test_publish_using_both_queue_params_and_exchange_params(self):
        """
        Verify that a ValueError is raised if both queue and exchange
        parameters are provided to publish.
        """
        # Prep
        queue_params = QueueParams("queue")
        exchange_params = ExchangeParams("exchange")

        # Run test + assertions
        with self.assertRaises(ValueError):
            self.producer.publish(b"body",
                                  exchange_params=exchange_params,
                                  queue_params=queue_params)

    def test_publish_using_neither_queue_params_and_exchange_params(self):
        """
        Verify that a ValueError is raised if neither queue nor exchange
        parameters are provided to publish.
        """
        # Run test + assertions
        with self.assertRaises(ValueError):
            self.producer.publish(b"body")

    def test_publish_using_exchange_and_routing_key(self):
        """
        Verify that publishing using both exchange and routing key has the
        expected behavior.
        """
        # Prep
        exchange_params = ExchangeParams("exchange")
        routing_key = "routing_key"

        # Run test + assertions
        self.producer.publish(b"body",
                              exchange_params=exchange_params,
                              routing_key=routing_key)
        self.producer.declare_exchange.assert_called_with(
            exchange_params, callback=ANY
        )

        self.producer.on_exchange_declared(b"body",
                                           exchange_params,
                                           None,
                                           routing_key=routing_key,
                                           publish_params=None)
        self.producer.basic_publish.assert_called_with(
            b"body",
            exchange=exchange_params.exchange,
            routing_key=routing_key,
            publish_params=None
        )

    def test_publish_including_publish_params(self):
        """
        Verify that PublishParams are passed as expected when calling publish.
        """
        # Prep
        exchange_params = ExchangeParams("exchange")
        publish_params = PublishParams()
        routing_key = "routing_key"

        # Run test + assertions
        self.producer.publish(b"body",
                              exchange_params=exchange_params,
                              routing_key=routing_key,
                              publish_params=publish_params)
        self.producer.declare_exchange.assert_called_with(
            exchange_params, callback=ANY
        )

        self.producer.on_exchange_declared(b"body",
                                           exchange_params,
                                           None,
                                           routing_key="",
                                           publish_params=publish_params)
        self.producer.basic_publish.assert_called_with(
            b"body",
            exchange=exchange_params.exchange,
            routing_key="",
            publish_params=publish_params
        )

    def test_publish_is_delayed_until_producer_connection_ready(self):
        """
        Verify that the producer buffers messages that are to be sent when the
        producer connection is not in a ready state, and that the buffered
        messages are sent upon the producer becoming ready.
        """
        # Prep
        exchange_params = ExchangeParams("exchange")
        frame_mock = Mock()
        frame_mock.method.queue = "queue"
        queue_params = QueueParams("queue")

        # Run test + assertions
        self.producer.on_close()
        self.producer.publish(b"body", exchange_params=exchange_params)
        self.producer.publish(b"body", queue_params=queue_params)

        self.assertEqual(2, len(self.producer._buffered_messages))

        self.producer.on_ready()
        self.producer.declare_exchange.assert_called_with(
            exchange_params, callback=ANY
        )
        self.producer.declare_queue.assert_called_with(
            queue_params, callback=ANY
        )
