import unittest

from unittest.mock import patch, Mock, ANY, call

from rabbitmq_client import (
    ExchangeParams,
    ConsumeParams,
    QueueParams,
    ConsumeOK,
    RMQConsumer
)
from rabbitmq_client.consumer import _gen_consume_key


class TestConsumeKeyGeneration(unittest.TestCase):

    def test_consume_key_gen(self):
        """Verify the consume key gen works as intended"""
        self.assertEqual(
            "queue|exchange|routing_key",
            _gen_consume_key(
                "queue", "exchange", "routing_key"
            )
        )
        self.assertEqual(
            "exchange|routing_key",
            _gen_consume_key(
                None, "exchange", "routing_key"
            )
        )
        self.assertEqual(
            "queue|routing_key",
            _gen_consume_key(
                "queue", None, "routing_key"
            )
        )
        self.assertEqual(
            "queue|exchange|complex.routing.key",
            _gen_consume_key(
                "queue",
                "exchange",
                "complex.routing.key"
            )
        )
        self.assertEqual(
            "exchange",
            _gen_consume_key(
                "",
                "exchange",
                None
            )
        )
        self.assertEqual(
            "queue|exchange",
            _gen_consume_key(
                "queue",
                "exchange",
                None
            )
        )


class TestConsumeInterface(unittest.TestCase):

    @patch("rabbitmq_client.consumer.RMQConnection.start")
    def setUp(self, _connection_start) -> None:
        """Setup to run before each test case."""
        self.consumer = RMQConsumer()
        self.consumer.start()
        self.consumer.on_ready()  # Fake connection getting ready

        self.consumer.declare_queue = Mock()
        self.consumer.declare_exchange = Mock()
        self.consumer.bind_queue = Mock()
        self.consumer.basic_consume = Mock()

    def set_up_confirmed_consume(self, auto_ack=False) -> str:
        """Helper that sets up queue-only confirmed consume."""
        queue_params = QueueParams("queue")
        self.consumer.consume(ConsumeParams(lambda _: ..., auto_ack=auto_ack),
                              queue_params=queue_params)
        frame_mock = Mock()
        frame_mock.method.consumer_tag = "123"
        self.consumer.on_consume_ok(frame_mock, queue_params=queue_params)

        return "123"


    def test_consume_exchange_only(self):
        """Verify possibility to consume from an exchange."""
        # Prep
        def on_msg(): pass
        consume = ConsumeParams(on_msg)
        exchange = ExchangeParams("exchange")

        # Run test
        self.consumer.consume(consume, exchange_params=exchange)

        #
        # Assertions

        # Verify consume instance exists in consumer instance
        consume_key = _gen_consume_key(exchange=exchange.exchange)
        consume_instance = self.consumer._consumes[consume_key]
        self.assertEqual(consume_instance.consume_params, consume)
        self.assertEqual(consume_instance.exchange_params, exchange)

        # Always starts with queue declare, can't verify call params since
        # queue is autogenerated if not provided, and the other arg is a
        # callback constructed from functools.partial.
        self.consumer.declare_queue.assert_called()

    def test_consume_queue_only(self):
        """Verify possibility to consume from a queue."""
        # Prep
        def on_msg(): pass
        consume = ConsumeParams(on_msg)
        queue = QueueParams("queue")

        # Run test
        self.consumer.consume(consume, queue_params=queue)

        #
        # Assertions
        consume_key = _gen_consume_key(queue=queue.queue)
        consume_instance = self.consumer._consumes[consume_key]
        self.assertEqual(consume_instance.consume_params, consume)
        self.assertEqual(consume_instance.queue_params, queue)
        self.assertEqual(consume_instance.exchange_params, None)
        self.consumer.declare_queue.assert_called_with(queue, callback=ANY)

    def test_consume_both_queue_and_exchange(self):
        """
        Verify possibility to provide both an exchange and a queue to the
        consume method.
        """
        # Prep
        def on_msg(): pass
        consume = ConsumeParams(on_msg)
        queue = QueueParams("queue")
        exchange = ExchangeParams("exchange")

        # Run test
        self.consumer.consume(
            consume, queue_params=queue, exchange_params=exchange
        )

        # Assertions
        consume_key = _gen_consume_key(queue=queue.queue,
                                       exchange=exchange.exchange)
        consume_instance = self.consumer._consumes[consume_key]
        self.assertEqual(consume_instance.consume_params, consume)
        self.assertEqual(consume_instance.queue_params, queue)
        self.assertEqual(consume_instance.exchange_params, exchange)
        self.consumer.declare_queue.assert_called_with(queue, callback=ANY)

    def test_consume_queue_exchange_and_routing_key(self):
        """
        Verify possibility to provide both an exchange, routing_key and a queue
        to the consume method.
        """
        # Prep
        def on_msg(): pass
        consume = ConsumeParams(on_msg)
        queue = QueueParams("queue")
        exchange = ExchangeParams("exchange")

        # Run test
        self.consumer.consume(consume,
                              queue_params=queue,
                              exchange_params=exchange,
                              routing_key="routing_key")

        # Assertions
        consume_key = _gen_consume_key(queue=queue.queue,
                                       exchange=exchange.exchange,
                                       routing_key="routing_key")
        consume_instance = self.consumer._consumes[consume_key]
        self.assertEqual(consume_instance.consume_params, consume)
        self.assertEqual(consume_instance.queue_params, queue)
        self.assertEqual(consume_instance.exchange_params, exchange)
        self.assertEqual(consume_instance.routing_key, "routing_key")
        self.consumer.declare_queue.assert_called_with(queue, callback=ANY)


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


# noinspection DuplicatedCode
class TestConsumer(unittest.TestCase):
    """
    Test the new (2021) RMQConsumer class, verify its interface methods can be
    used as advertised and in different combinations.
    """

    @patch("rabbitmq_client.consumer.RMQConnection.start")
    def setUp(self, _connection_start) -> None:
        """Setup to run before each test case."""
        self.consumer = RMQConsumer()
        self.consumer.start()
        self.consumer.on_ready()  # Fake connection getting ready

        self.consumer.declare_queue = Mock()
        self.consumer.declare_exchange = Mock()
        self.consumer.bind_queue = Mock()
        self.consumer.basic_consume = Mock()

    def set_up_confirmed_consume(self, auto_ack=False) -> str:
        """Helper that sets up queue-only confirmed consume."""
        queue_params = QueueParams("queue")
        self.consumer.consume(ConsumeParams(lambda _: ..., auto_ack=auto_ack),
                              queue_params=queue_params)
        frame_mock = Mock()
        frame_mock.method.consumer_tag = "123"
        self.consumer.on_consume_ok(frame_mock, queue_params=queue_params)

        return "123"

    def test_consumer_readiness(self):
        """Verify the consumer's ready property changes as expected."""
        self.assertTrue(self.consumer.ready)
        self.consumer.on_close()
        self.assertFalse(self.consumer.ready)
        self.consumer.on_ready()
        self.assertTrue(self.consumer.ready)
        self.consumer.on_close(permanent=True)
        self.assertFalse(self.consumer.ready)

    def test_consumer_error_handling(self):
        """Verify on_error results in correct behavior..."""
        with self.assertRaises(NotImplementedError):
            self.consumer.on_error()

    def test_on_queue_declared_no_exchange(self):
        """
        Verify that the correct action follows a successful queue declare.
        """
        # Prep
        self.consumer.basic_consume = Mock()
        frame = Mock()
        consume_params = ConsumeParams(lambda _: ...)
        queue_params = QueueParams("queue")

        # Run test
        self.consumer.on_queue_declared(frame,
                                        consume_params=consume_params,
                                        queue_params=queue_params)

        # Assertions
        self.consumer.basic_consume.assert_called_with(
            consume_params,
            on_message_callback_override=self.consumer.on_msg,
            callback=ANY)

    def test_on_queue_declared_exchange(self):
        """
        Verify that the correct action follows a successful queue declare when
        exchange is specified.
        """
        # Prep
        self.consumer.declare_exchange = Mock()
        frame = Mock()
        consume_params = ConsumeParams(lambda _: ...)
        queue_params = QueueParams("queue")
        exchange_params = ExchangeParams("exchange")

        # Run test
        self.consumer.on_queue_declared(frame,
                                        consume_params=consume_params,
                                        queue_params=queue_params,
                                        exchange_params=exchange_params)

        # Assertions
        self.assertTrue(isinstance(consume_params.queue, Mock))
        self.consumer.declare_exchange.assert_called_with(
            exchange_params, callback=ANY)

    def test_on_exchange_declared(self):
        """
        Verify that the correct action follows a successful exchange
        declaration.
        """
        # Prep
        self.consumer.bind_queue = Mock()
        exchange_params = ExchangeParams("exchange")
        consume_params = ConsumeParams(lambda _: ...)
        queue_params = QueueParams("queue")

        # Run test
        self.consumer.on_exchange_declared(exchange_params,
                                           None,
                                           consume_params=consume_params,
                                           queue_params=queue_params)

        # Assertions
        call_args = self.consumer.bind_queue.call_args
        queue_bind_params = call_args.args[0]
        self.consumer.bind_queue.assert_called_with(
            queue_bind_params, callback=ANY
        )
        self.assertEqual(queue_bind_params.queue, consume_params.queue)
        self.assertEqual(queue_bind_params.exchange, exchange_params.exchange)
        self.assertEqual(queue_bind_params.routing_key, None)

    def test_on_queue_bound(self):
        """
        Verify that the correct action follows a successful queue bind
        operation.
        """
        # Prep
        self.consumer.basic_consume = Mock()
        frame = Mock()
        consume_params = ConsumeParams(lambda _: ...)
        queue_params = QueueParams("queue")

        # Run test
        self.consumer.on_queue_bound(frame,
                                     consume_params=consume_params,
                                     queue_params=queue_params,
                                     exchange="exchange",
                                     routing_key=None)

        # Assertions
        self.consumer.basic_consume.assert_called_with(
            consume_params,
            on_message_callback_override=self.consumer.on_msg,
            callback=ANY
        )

    def test_on_consume_ok(self):
        """
        Verify that the correct actions follows a consume OK.
        """
        # Prep
        on_message_callback_called = False

        def on_message_callback(msg):
            if not isinstance(msg, ConsumeOK):
                return

            nonlocal on_message_callback_called
            on_message_callback_called = True

        consume_params = ConsumeParams(on_message_callback)
        queue_params = QueueParams("queue")
        frame = Mock()
        self.consumer.consume(consume_params, queue_params=queue_params)

        # Run test
        self.consumer.on_consume_ok(frame,
                                    queue_params=queue_params)

        # Assertions
        consume_instance = self.consumer._consumes[
            _gen_consume_key(queue=queue_params.queue)
        ]
        self.assertEqual(consume_instance.consumer_tag,
                         frame.method.consumer_tag)
        self.assertTrue(on_message_callback_called)  # Called with ConsumeOK

    def test_on_ready_starts_consumes(self):
        """
        Verify that a call to on_ready results in re-issuing of all active
        consumes.
        """
        # Prep
        self.consumer._handle_consume = Mock()

        queue_params = QueueParams("queue")
        exchange_params = ExchangeParams("exchange")

        self.consumer.consume(ConsumeParams(lambda _: ...),
                              queue_params=queue_params)
        self.consumer.consume(ConsumeParams(lambda _: ...),
                              exchange_params=exchange_params)

        # Run test
        self.consumer.on_ready()

        # Assertions
        self.consumer._handle_consume.assert_has_calls(
            (call(ANY, queue_params, None, None),
             call(ANY, None, exchange_params, None),)
        )

    def test_on_consume_ok_callback_raises_exception(self):
        """
        Verify that an exception raised from the consume OK notification does
        not crash the consumer.
        """
        # Prep
        def crappy_callback(msg):
            # Random exception chosen, ANY exception shall be possible to
            # handle.
            raise ValueError

        queue_params = QueueParams("queue")

        self.consumer.consume(ConsumeParams(crappy_callback),
                              queue_params=queue_params)

        # Run test
        # Assertion is that no unhandled exception happens :-)
        self.consumer.on_consume_ok(Mock(), queue_params=queue_params)

    def test_on_consume_ok_on_close_consumes_entry_handling(self):
        """
        Verify receiving a call to on_consume_ok creates as additional entry in
        the _consumes dict to allow lookups of consume parameters using
        consumer tags, and that on_close causes the deletion of this entry.
        """
        # Prep
        consumer_tag = self.set_up_confirmed_consume()

        # Run test + assertions
        self.assertNotEqual(self.consumer._consumes.get(consumer_tag, None),
                            None)  # Entry created
        self.consumer.on_close(permanent=True)
        self.assertEqual(self.consumer._consumes.get(consumer_tag, None),
                         None)  # Now it should be gone

    def test_on_msg(self):
        """
        Verify on_msg calls result in a call to the consumer's function as
        well as long as a matching consumer tag entry is found.
        """
        # Prep
        on_message_callback_called = False

        def on_message_callback(msg, ack=None):
            if not msg == b'body' or isinstance(msg, ConsumeOK):
                return

            nonlocal on_message_callback_called
            on_message_callback_called = True

        queue_params = QueueParams("queue")
        self.consumer.consume(ConsumeParams(on_message_callback),
                              queue_params=queue_params)
        frame_mock = Mock()
        frame_mock.method.consumer_tag = "123"
        self.consumer.on_consume_ok(frame_mock, queue_params=queue_params)
        deliver_mock = Mock()
        deliver_mock.consumer_tag = "123"

        # Run test
        self.consumer.on_msg(Mock(), deliver_mock, Mock(), b"body")

        # Assertions
        self.assertTrue(on_message_callback_called)

    def test_on_msg_callback_raises_exception(self):
        """
        Verify that exceptions raised from the consumer callback as a result of
        an on_msg invocation does not lead to an exception in RMQConsumer.
        """
        # Prep
        def on_message_callback(msg):
            if isinstance(msg, ConsumeOK):
                return

            raise ValueError

        self.consumer.consume(ConsumeParams(on_message_callback),
                              queue_params=QueueParams("queue"))
        frame_mock = Mock()
        frame_mock.method.consumer_tag = "123"
        self.consumer.on_consume_ok(frame_mock,
                                    queue_params=QueueParams("queue"))
        deliver_mock = Mock()
        deliver_mock.consumer_tag = "123"

        # Run test, assertion is that no exception is uncaught
        self.consumer.on_msg(Mock(), deliver_mock, Mock(), b"body")

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

    def test_manual_ack_consume(self):
        consumer_tag = self.set_up_confirmed_consume(auto_ack=False)

        def on_msg(_msg, ack=None):
            ack()

        consume = self.consumer._consumes.get(consumer_tag)
        consume.consume_params.on_message_callback = on_msg

        basic_deliver = Mock()
        basic_deliver.consumer_tag = consumer_tag
        basic_deliver.delivery_tag = 123
        channel_mock = Mock()
        ack_mock = Mock()
        channel_mock.basic_ack = ack_mock
        self.consumer.on_msg(channel_mock, basic_deliver, Mock(), b"body")

        ack_mock.assert_called_with(delivery_tag=123)
