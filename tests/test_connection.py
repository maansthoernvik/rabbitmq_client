import unittest

from unittest.mock import Mock, patch

from pika.exceptions import (
    ConnectionClosedByBroker,
    StreamLostError,
    ConnectionWrongStateError
)
from rabbitmq_client import (
    RMQConnection,
    QueueParams,
    ExchangeParams,
    QueueBindParams,
    ConsumeParams,
    DEFAULT_EXCHANGE,
    PublishParams
)
from tests.defs import NotAThread


class ConnectionImplementer(RMQConnection):
    """Test class, implementing the RMQConnection interface."""

    def __init__(self, connection_parameters=None):
        super().__init__(connection_parameters=connection_parameters)

        # Override thread usage to avoid multithreading in unittest. The fake
        # Thread replicates the needed Thread methods.
        self._connection_thread = NotAThread(target=self._connect)

    def on_ready(self):
        pass

    def on_close(self):
        pass

    def on_error(self):
        pass


class TestConnectionBase(unittest.TestCase):
    """
    Verify the RMQConnection class can be subclassed and starts a connection
    towards RabbitMQ as intended. Also verify reconnect handling and closure
    handling of both channels and connections.

    SelectConnection is patched in to replace the real pika.SelectConnection
    with a MagicMock object. Thread is sometimes patched to allow assertion
    checking for when they are started and what arguments are provided to
    them. For tests that require the calls made by a Thread to be done more
    than the initial start, NotAThread is patched in to ensure that re-
    initialization makes NotAThread be used in place of threading.Thread.
    """

    def setUp(self) -> None:
        self.conn_imp = ConnectionImplementer()

    @patch("rabbitmq_client.connection.SelectConnection")
    def test_successful_start(self, _select_connection):
        """
        Verify successful connection and channel establishment.
        """
        # Setup
        self.conn_imp.on_ready = Mock()

        # Run test
        self.conn_imp.start()
        self.conn_imp.on_connection_open(None)
        channel_mock = Mock()
        self.conn_imp.on_channel_open(channel_mock)

        # Assertions
        self.conn_imp._connection.ioloop.start.assert_called()
        self.conn_imp._connection.channel.assert_called_with(
            on_open_callback=self.conn_imp.on_channel_open
        )
        self.assertEqual(self.conn_imp._reconnect_attempts, 0)
        self.assertEqual(channel_mock, self.conn_imp._channel)
        self.conn_imp._channel.add_on_close_callback.assert_called_with(
            self.conn_imp.on_channel_closed
        )
        self.conn_imp.on_ready.assert_called()

    @patch("rabbitmq_client.connection.SelectConnection")
    def test_unsuccessful_start(self, _select_connection):
        """
        Verify unsuccessful connection establishment.
        """
        # Setup
        self.conn_imp._reconnect = Mock()
        self.conn_imp.start()

        # Run test
        self.conn_imp.on_connection_open_error(None, None)

        # Assertions
        self.conn_imp._reconnect.assert_called()

    @patch("rabbitmq_client.connection.SelectConnection")
    def test_connection_closed_by_broker(self, _select_connection):
        """
        Verify connection closed by broker.
        """
        # Setup
        self.conn_imp.on_close = Mock()
        self.conn_imp._reconnect = Mock()
        self.conn_imp.start()
        self.conn_imp.on_connection_open(None)
        self.conn_imp.on_channel_open(Mock())

        # Run test
        self.conn_imp.on_connection_closed(
            None, ConnectionClosedByBroker(403, "closed by broker")
        )

        # Assertions
        self.conn_imp.on_close.assert_called()
        self.conn_imp._connection.ioloop.stop.assert_called()
        self.conn_imp._reconnect.assert_called()

    @patch("rabbitmq_client.connection.SelectConnection")
    def test_connection_closed_stream_lost(self, _select_connection):
        """
        Verify connection closed due to StreamLostError.
        """
        # Setup
        self.conn_imp.on_close = Mock()
        self.conn_imp._reconnect = Mock()
        self.conn_imp.start()
        self.conn_imp.on_connection_open(None)
        self.conn_imp.on_channel_open(Mock())

        # Run test
        self.conn_imp.on_connection_closed(
            None, StreamLostError()
        )

        # Assertions
        self.conn_imp.on_close.assert_called()
        self.conn_imp._connection.ioloop.stop.assert_called()
        self.conn_imp._reconnect.assert_called()

    @patch("rabbitmq_client.connection.SelectConnection")
    def test_stop_connection(self, _select_connection):
        """
        Verify connection stop.
        """
        # Setup
        self.conn_imp.on_close = Mock()
        self.conn_imp._reconnect = Mock()
        self.conn_imp.start()
        self.conn_imp.on_connection_open(None)
        self.conn_imp.on_channel_open(Mock())

        # Run test
        self.conn_imp.stop()
        self.conn_imp.on_connection_closed(None, None)

        # Assertions
        self.assertTrue(self.conn_imp._closing)
        self.conn_imp._connection.close.assert_called()
        self.conn_imp.on_close.assert_called()
        self.conn_imp._connection.ioloop.stop.assert_called()
        self.conn_imp._reconnect.assert_not_called()

    def test_stop_connection_already_stopped(self):
        """
        Verify connection stop when already stopped.
        """
        # Setup
        self.conn_imp._connection = Mock()
        self.conn_imp._connection.close.side_effect = (
            ConnectionWrongStateError
        )

        # Run test
        self.conn_imp.stop()

        # Assertions
        self.assertTrue(self.conn_imp._closing)

    @patch("rabbitmq_client.connection.SelectConnection")
    @patch("rabbitmq_client.connection.Thread", new=NotAThread)
    def test_restart_connection(self, _select_connection):
        """
        Verify connection restart.
        """
        # Setup
        self.conn_imp.on_close = Mock()
        self.conn_imp.on_ready = Mock()
        self.conn_imp._connection = Mock()

        # Run test

        # Restart
        self.conn_imp.restart()
        self.assertTrue(self.conn_imp._restarting)  # Must check before close
        self.conn_imp._connection.close.assert_called()

        # Connection closed
        # Because connection is refreshed, save ref to old connection.
        previous_connection = self.conn_imp._connection
        self.conn_imp.on_connection_closed(None, None)
        self.conn_imp.on_close.assert_called()
        previous_connection.ioloop.stop.assert_called()
        self.assertFalse(self.conn_imp._restarting)
        self.conn_imp._connection.ioloop.start.assert_called()

        # Connection opened
        self.conn_imp.on_connection_open(None)
        self.conn_imp._connection.channel.assert_called_with(
            on_open_callback=self.conn_imp.on_channel_open
        )

        # Channel opened
        channel_mock = Mock()
        self.conn_imp.on_channel_open(channel_mock)
        self.assertEqual(self.conn_imp._reconnect_attempts, 0)
        self.assertEqual(self.conn_imp._channel, channel_mock)
        self.conn_imp._channel.add_on_close_callback.assert_called()
        self.conn_imp.on_ready.assert_called()

    @patch("rabbitmq_client.connection.Thread")
    def test_reconnect_first_attempt(self, thread):
        """
        Verify first reconnect.
        """
        # Run test
        self.conn_imp._reconnect()

        # Assertions
        thread.assert_called_with(
            target=self.conn_imp._connect,
            daemon=True,
        )
        self.conn_imp._connection_thread.start.assert_called()
        self.assertEqual(self.conn_imp._reconnect_attempts, 1)

    @patch("rabbitmq_client.connection.Timer")
    def test_reconnect_attempt_less_than_9(self, timer):
        """
        Verify second to ninth reconnect attempts.
        """
        # Setup
        self.conn_imp._reconnect_attempts = 1

        # Run test
        self.conn_imp._reconnect()

        # Assertions
        timer.assert_called_with(
            1, self.conn_imp._connection_thread.start
        )
        self.assertEqual(self.conn_imp._reconnect_attempts, 2)

    @patch("rabbitmq_client.connection.Timer")
    def test_reconnect_attempt_9_or_more(self, timer):
        """
        Verify second to ninth reconnect attempts.
        """
        # Setup
        self.conn_imp._reconnect_attempts = 9

        # Run test
        self.conn_imp._reconnect()

        # Assertions
        timer.assert_called_with(
            30, self.conn_imp._connection_thread.start
        )
        self.assertEqual(self.conn_imp._reconnect_attempts, 10)

    def test_channel_closed(self):
        """
        Verify channel closed.
        """
        # Setup
        self.conn_imp.on_close = Mock()

        # Run test
        self.conn_imp.on_channel_closed(None, None)

        # Assertions
        self.conn_imp.on_close.assert_called()

    def test_channel_closed_permanently(self):
        """
        Verify channel closed permanently.
        """
        # Setup
        self.conn_imp.on_close = Mock()
        self.conn_imp.stop = Mock()
        reason_mock = Mock()
        reason_mock.reply_code = 406  # PRECONDITION FAILED

        # Run test
        self.conn_imp.on_channel_closed(None, reason_mock)

        # Assertions
        self.conn_imp.on_close.assert_called_with(permanent=True)
        self.conn_imp.stop.assert_called()

    @patch("rabbitmq_client.connection.SelectConnection")
    def test_stop_failed_start_connection(self, _select_connection):
        """
        Verify stopping a connection that was never successfully started.
        """
        # Setup
        self.conn_imp.start()

        # Run test
        self.conn_imp.stop()
        self.conn_imp.on_connection_open_error(None, None)

        # Assertions
        self.assertTrue(self.conn_imp._closing)
        self.assertEqual(self.conn_imp._reconnect_attempts, 0)

    def test_restart_closed_connection(self):
        """
        Verify restarting a closed connection.
        """
        # Setup
        self.conn_imp._connection = Mock()
        self.conn_imp._connection.close.side_effect = (
            ConnectionWrongStateError()
        )

        # Run test
        self.conn_imp.restart()

        # Assertions
        self.assertFalse(self.conn_imp._restarting)


class TestConnectionDeclarations(unittest.TestCase):
    """
    Verify the RMQConnection's methods for declaring, binding, and consuming
    start the intended flows. For example, declaring an exchange and supplying
    queue parameters should result in a queue being declared after the exchange
    has been declared successfully. Also, including consume parameters shall
    lead to 'basic_consume' being issued.
    """

    @patch("rabbitmq_client.connection.SelectConnection")
    def setUp(self, _select_connection) -> None:
        """
        Set up connection to be in a started state, where both connection and
        channel objects are mock instances.
        """
        self.conn_imp = ConnectionImplementer()
        self.conn_imp.start()
        self.conn_imp.on_connection_open(None)
        self.channel_mock = Mock()
        self.conn_imp.on_channel_open(self.channel_mock)

    def test_declare_queue(self):
        """
        Verify declaring a queue.
        """
        # Prep
        queue_params = QueueParams("queue")
        def on_queue_declared(): ...

        # Test
        self.conn_imp.declare_queue(queue_params, callback=on_queue_declared)

        # Assert
        self.conn_imp._channel.queue_declare.assert_called_with(
            queue_params.queue,
            durable=queue_params.durable,
            exclusive=queue_params.exclusive,
            auto_delete=queue_params.auto_delete,
            arguments=queue_params.arguments,
            callback=on_queue_declared
        )

    def test_declare_exchange(self):
        """
        Verify declaring an exchange.
        """
        # Prep
        exchange_params = ExchangeParams("exchange")
        def on_exchange_declared(): ...

        # Test
        self.conn_imp.declare_exchange(
            exchange_params, callback=on_exchange_declared
        )

        # Assert
        self.conn_imp._channel.exchange_declare.assert_called_with(
            exchange_params.exchange,
            exchange_type=exchange_params.exchange_type,
            durable=exchange_params.durable,
            auto_delete=exchange_params.auto_delete,
            internal=exchange_params.internal,
            arguments=exchange_params.arguments,
            callback=on_exchange_declared
        )

    def test_bind_queue(self):
        """
        Verify binding a queue to an exchange.
        """
        # Prep
        queue_bind_params = QueueBindParams(
            "queue", "exchange", routing_key="routing_key"
        )
        def on_queue_bound(): ...

        # Test
        self.conn_imp.bind_queue(
            queue_bind_params,
            callback=on_queue_bound
        )

        # Assert
        self.conn_imp._channel.queue_bind.assert_called_with(
            queue_bind_params.queue,
            queue_bind_params.exchange,
            routing_key=queue_bind_params.routing_key,
            arguments=queue_bind_params.arguments,
            callback=on_queue_bound
        )

    def test_consume_from_queue(self):
        """
        Verify consuming from a queue with the on_message_callback_override.
        """
        # Prep
        def consume_on_msg(): ...
        consume_params = ConsumeParams(consume_on_msg)
        def consumer_on_msg(): ...
        def on_consume_ok(): ...

        # Test
        self.conn_imp.basic_consume(
            consume_params,
            on_message_callback_override=consumer_on_msg,
            callback=on_consume_ok
        )

        # Assert
        self.conn_imp._channel.basic_consume.assert_called_with(
            consume_params.queue,
            consumer_on_msg,
            auto_ack=consume_params.auto_ack,
            exclusive=consume_params.exclusive,
            consumer_tag=consume_params.consumer_tag,
            arguments=consume_params.arguments,
            callback=on_consume_ok
        )

    def test_consume_from_queue_wo_override(self):
        """
        Verify consuming from a queue without the callback override.
        """

        # Prep
        def consume_on_msg(): ...
        consume_params = ConsumeParams(consume_on_msg)
        def on_consume_ok(): ...

        # Test
        self.conn_imp.basic_consume(
            consume_params,
            callback=on_consume_ok
        )

        # Assert
        self.conn_imp._channel.basic_consume.assert_called_with(
            consume_params.queue,
            consume_params.on_message_callback,
            auto_ack=consume_params.auto_ack,
            exclusive=consume_params.exclusive,
            consumer_tag=consume_params.consumer_tag,
            arguments=consume_params.arguments,
            callback=on_consume_ok
        )

    def test_basic_publish(self):
        """Verify calls to basic_publish are handled as expected."""
        # Test
        self.conn_imp.basic_publish(b"body", routing_key="queue")

        # Assert
        self.conn_imp._channel.basic_publish.assert_called_with(
            DEFAULT_EXCHANGE,
            "queue",
            b"body",
            properties=None,
            mandatory=False
        )

    def test_basic_publish_with_params(self):
        """
        Verify calls to basic_publish are handled as expected when including
        publish parameters.
        """
        # Prep
        publish_params = PublishParams(mandatory=True)

        # Test
        self.conn_imp.basic_publish(b"body",
                                    exchange="logging",
                                    routing_key="log.warning",
                                    publish_params=publish_params)

        # Assert
        self.conn_imp._channel.basic_publish.assert_called_with(
            "logging",
            "log.warning",
            b"body",
            properties=None,
            mandatory=True
        )
