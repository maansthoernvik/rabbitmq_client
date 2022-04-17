import unittest

from typing import Union
from unittest.mock import Mock, patch, ANY
from pika.spec import Basic

from rabbitmq_client.connection import MandatoryError, DeclarationError
from rabbitmq_client import (
    RMQProducer,
    ExchangeParams,
    QueueParams,
    PublishParams,
    ConfirmModeOK,
    DeliveryError,
    DEFAULT_EXCHANGE
)


class TestPublishInterface(unittest.TestCase):

    @patch("rabbitmq_client.producer.RMQProducer.start")
    def setUp(self, _start) -> None:
        """Setup to run before each test case."""
        self.producer = RMQProducer()
        self.producer.start()
        self.producer.on_ready()  # Fake connection getting ready

        self.producer.declare_queue = Mock()
        self.producer.declare_exchange = Mock()
        self.producer.bind_queue = Mock()
        self.producer.basic_publish = Mock()

    def test_publish_using_exchange_params(self):
        """Verify possibility to publish to an exchange."""
        # Prep
        exchange_params = ExchangeParams("exchange")

        # Run test + assertions
        self.producer.publish(b"body", exchange_params=exchange_params)
        self.producer.declare_exchange.assert_called_with(
            exchange_params, ANY
        )

        self.producer.when_exchange_declared(b"body",
                                             exchange_params,
                                             "")
        self.producer.basic_publish.assert_called_with(
            b"body",
            exchange_params.exchange,
            "",
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
            queue_params, ANY
        )

        self.producer.when_queue_declared(b"body",
                                          "queue")
        self.producer.basic_publish.assert_called_with(
            b"body",
            DEFAULT_EXCHANGE,
            queue_params.queue,
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
            exchange_params, ANY
        )

        self.producer.when_exchange_declared(b"body",
                                             exchange_params,
                                             routing_key)
        self.producer.basic_publish.assert_called_with(
            b"body",
            exchange_params.exchange,
            routing_key,
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
            exchange_params, ANY
        )

        self.producer.when_exchange_declared(b"body",
                                             exchange_params,
                                             "",
                                             publish_params=publish_params)
        self.producer.basic_publish.assert_called_with(
            b"body",
            exchange_params.exchange,
            "",
            publish_params=publish_params
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

    def test_publish_is_delayed_until_producer_connection_ready(self):
        """
        Verify that the producer buffers messages that are to be sent when the
        producer connection is not in a ready state, and that the buffered
        messages are sent upon the producer becoming ready.
        """
        # Prep
        exchange_params = ExchangeParams("exchange")
        queue_params = QueueParams("queue")

        # Run test + assertions
        self.producer.on_close()
        self.producer.publish(b"body", exchange_params=exchange_params)
        self.producer.publish(b"body", queue_params=queue_params)

        self.assertEqual(2, len(self.producer._buffered_messages))

        self.producer.on_ready()
        self.producer.declare_exchange.assert_called_with(
            exchange_params, ANY
        )
        self.producer.declare_queue.assert_called_with(
            queue_params, ANY
        )


class TestConfirmMode(unittest.TestCase):
    """
    Test the confirm mode functionality of RMQProducer.
    """

    @patch("rabbitmq_client.producer.RMQProducer.start")
    def setUp(self, _connection_start) -> None:
        """Setup to run before each test case."""
        self.producer = RMQProducer()
        self.producer.start()
        self.producer.on_ready()  # Fake connection getting ready

        self.producer.confirm_delivery = Mock()
        self.producer.declare_exchange = Mock()
        self.producer.basic_publish = Mock()

    def test_push_confirm_mode_active_using_connection_hooks(self):
        self.assertFalse(self.producer._confirm_mode_active)

        self.producer.activate_confirm_mode(lambda _: ...)
        self.producer.on_confirm_select_ok(Mock())
        self.assertTrue(self.producer._confirm_mode_active)

        self.producer.on_close()
        self.assertFalse(self.producer._confirm_mode_active)

    def test_activate_confirm_mode(self):
        """
        Verify that confirm mode, when activated, yields the expected state.
        """
        # Prep
        confirm_mode_started = False

        def notify_callback(confirm):
            nonlocal confirm_mode_started

            if isinstance(confirm, ConfirmModeOK):
                confirm_mode_started = True

        # Test
        self.producer.activate_confirm_mode(notify_callback)
        self.producer.confirm_delivery.assert_called_with(
            self.producer.on_delivery_confirmed,
            callback=self.producer.on_confirm_select_ok
        )

        self.assertFalse(self.producer._confirm_mode_active)
        self.producer.on_confirm_select_ok(Mock())

        # Assert
        self.assertTrue(confirm_mode_started)
        self.assertTrue(self.producer._confirm_mode_active)

    def test_confirm_mode_publish_key_returned_by_publish(self):
        """
        Verify that a publish key is generated and returned by publish if
        confirm mode is activated.
        """
        # Prep
        publish_key = None

        def on_confirm(delivery_notification):
            if delivery_notification == publish_key:
                return
            elif isinstance(delivery_notification, ConfirmModeOK):
                return

            self.fail("delivery notification did not match the generated "
                      "publish key")

        self.producer.activate_confirm_mode(on_confirm)
        self.producer.confirm_delivery.assert_called_with(
            self.producer.on_delivery_confirmed,
            callback=self.producer.on_confirm_select_ok
        )

        self.producer.on_confirm_select_ok(None)

        # Test
        exchange_params = ExchangeParams("exchange")
        publish_key = self.producer.publish(
            b"body", exchange_params=exchange_params
        )
        self.assertTrue(isinstance(publish_key, str))

        self.producer.declare_exchange.assert_called()
        self.producer.when_exchange_declared(
            b"body", exchange_params, "", publish_key=publish_key
        )
        self.producer.basic_publish.assert_called_with(
            b"body", exchange_params.exchange, "", publish_params=None
        )
        # 1 is the delivery tag
        self.assertEqual(self.producer._unacked_publishes[
                             self.producer._next_delivery_tag-1
                         ], publish_key)

        frame = Mock()
        ack = Basic.Ack()
        ack.delivery_tag = 1
        frame.method = ack
        self.producer.on_delivery_confirmed(frame)
        self.assertEqual(len(self.producer._unacked_publishes.keys()), 0)

    def test_on_ready_initiates_confirm_mode_again(self):
        """
        Verify that after connectivity issues, on_ready will re-start confirm-
        mode.
        """
        # Prep
        self.producer.activate_confirm_mode(lambda _: ...)

        # Test
        self.producer.on_ready()

        # Assert
        self.producer.confirm_delivery.assert_called_with(
            self.producer.on_delivery_confirmed,
            callback=self.producer.on_confirm_select_ok
        )

    def test_delivery_not_acked_leads_to_delivery_error(self):
        """
        Verify that if a delivery confirmation is NOT an ack, a delivery error
        is propagated to the delivery notify callback.
        """
        # Prep
        got_error = False

        def on_confirm(error):
            nonlocal got_error
            got_error = True
            if isinstance(error, DeliveryError):
                self.assertEqual("123", error.publish_key)

        self.producer.activate_confirm_mode(on_confirm)
        self.producer._unacked_publishes[1] = "123"

        # Test
        frame = Mock()
        frame.method = Basic.Nack()
        frame.method.delivery_tag = 1
        self.producer.on_delivery_confirmed(frame)

        # Assert
        self.assertTrue(got_error)

    def test_activate_confirm_mode_idempotent(self):
        """
        Verify that calling activate_confirm_mode more than once has no effect,
        and will yield the same result as calling it once.
        """
        # Prep
        def on_confirm(): ...

        # Test + assert
        self.producer.activate_confirm_mode(on_confirm)
        self.producer.activate_confirm_mode(on_confirm)
        self.producer.confirm_delivery.assert_called_once()

    def test_confirm_mode_delays_buffer_until_confirm_mode_ok(self):
        """
        Verify that, if confirm mode has been activated, buffered publishes
        are not sent on 'on_ready', but rather 'on_confirm_select_ok'.
        """
        # Prep
        self.producer.activate_confirm_mode(lambda _: ...)
        self.producer.on_close()

        exchange_params = ExchangeParams("exchange")

        # Test + assertions
        _pub_key = self.producer.publish(b"body",
                                         exchange_params=exchange_params)
        self.assertNotEqual(_pub_key, None)
        self.producer.declare_exchange.assert_not_called()
        self.assertEqual(len(self.producer._buffered_messages), 1)

        self.producer.on_ready()
        self.producer.declare_exchange.assert_not_called()
        self.assertEqual(len(self.producer._buffered_messages), 1)

        self.producer.on_confirm_select_ok(Mock())
        self.producer.declare_exchange.assert_called()
        self.assertEqual(len(self.producer._buffered_messages), 0)

    def test_publish_before_confirm_mode_ok_buffers_the_publish(self):
        """
        Verify that any call to publish before confirm mode ok (when confirm
        mode has been activated) leads to the call being buffered and delayed
        until 'on_confirm_select_ok'.
        """
        # Prep
        self.producer.activate_confirm_mode(lambda _: ...)

        exchange_params = ExchangeParams("exchange")

        # Test + assertions
        _pub_key = self.producer.publish(b"body",
                                         exchange_params=exchange_params)
        self.assertNotEqual(_pub_key, None)
        self.producer.declare_exchange.assert_not_called()
        self.assertEqual(len(self.producer._buffered_messages), 1)

        self.producer.on_confirm_select_ok(Mock())
        self.producer.declare_exchange.assert_called()
        self.assertEqual(len(self.producer._buffered_messages), 0)

    def test_confirm_mode_unknown_delivery_tag(self):
        self.producer.activate_confirm_mode(lambda _: ...)

        # Test + assertions
        exchange_params = ExchangeParams("exchange")
        publish_key = self.producer.publish(
            b"body", exchange_params=exchange_params
        )
        self.assertTrue(isinstance(publish_key, str))

        self.producer.on_confirm_select_ok(Mock())
        self.producer.when_exchange_declared(b"body",
                                             exchange_params,
                                             "",
                                             publish_key=publish_key)

        self.assertEqual(publish_key,
                         self.producer._unacked_publishes[
                             self.producer._next_delivery_tag-1
                         ])

        delivery_confirmed_frame = Mock()
        delivery_confirmed_frame.method.delivery_tag = 1337

        # no crash occurs :-)
        self.producer.on_delivery_confirmed(delivery_confirmed_frame)

    def test_on_error_leads_to_delivery_error_if_mandatory_failure(self):
        mandatory_error = False

        def confirm_delivery(tag: Union[str,
                                        ConfirmModeOK,
                                        DeliveryError,
                                        MandatoryError]):
            nonlocal mandatory_error
            if isinstance(tag, MandatoryError):
                mandatory_error = True

        self.producer.activate_confirm_mode(confirm_delivery)
        self.producer.on_ready()  # -> confirm_mode
        self.producer.on_confirm_select_ok(Mock())

        # Some publish fails due to the mandatory flag being set
        self.producer.on_error(MandatoryError("exchange", "publish_key"))
        self.assertTrue(mandatory_error)

    def test_on_error_mandatory_error_works_outside_confirm_mode(self):
        """
        Outside confirm mode test of mandatory errors, in which case just log
        a warning.
        """
        self.producer.on_error(MandatoryError("exchange", "publish_key"))

    def test_on_error_declaration_error(self):
        """
        No crashes when receiving a declaration error.
        """
        self.producer.on_error(DeclarationError("a message"))
