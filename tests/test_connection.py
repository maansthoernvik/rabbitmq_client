import unittest

from unittest.mock import Mock, patch

from pika.exceptions import (
    ConnectionClosedByBroker,
    StreamLostError,
    ConnectionWrongStateError
)

from rabbitmq_client.new_connection import RMQConnection


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


class NotAThread:
    """
    Used to keep unittests single-threaded and avoid annoying wait-logic.
    Implements the needed Thread interface methods like start/join/etc.
    """
    def __init__(self, target=None):
        self.target = target
        self.alive = False

    def start(self):
        self.alive = True
        self.target()

    def is_alive(self):
        return self.alive


class TestConnection(unittest.TestCase):
    """
    Verify the RMQConnection class can be subclassed and works as intended.

    SelectConnection is patched in to replace the real pika.SelectConnection
    with a MagicMock object. Thread is sometimes patched to allow assertion
    checking for when they are started and what arguments are provided to
    them. For tests that require the calls made by a Thread to be done more
    than the initial start, NotAThread is patched in to ensure that re-
    initialization makes NotAThread be used in place of threading.Thread.
    """

    def setUp(self) -> None:
        self.conn_imp = ConnectionImplementer()

    @patch("rabbitmq_client.new_connection.SelectConnection")
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

    @patch("rabbitmq_client.new_connection.SelectConnection")
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

    @patch("rabbitmq_client.new_connection.SelectConnection")
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

    @patch("rabbitmq_client.new_connection.SelectConnection")
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

    @patch("rabbitmq_client.new_connection.SelectConnection")
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

    @patch("rabbitmq_client.new_connection.SelectConnection")
    @patch("rabbitmq_client.new_connection.Thread", new=NotAThread)
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

    @patch("rabbitmq_client.new_connection.Thread")
    def test_reconnect_first_attempt(self, thread):
        """
        Verify first reconnect.
        """
        # Run test
        self.conn_imp._reconnect()

        # Assertions
        thread.assert_called_with(
            target=self.conn_imp._connect
        )
        self.conn_imp._connection_thread.start.assert_called()
        self.assertEqual(self.conn_imp._reconnect_attempts, 1)

    @patch("rabbitmq_client.new_connection.Timer")
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

    @patch("rabbitmq_client.new_connection.Timer")
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

    @patch("rabbitmq_client.new_connection.SelectConnection")
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
