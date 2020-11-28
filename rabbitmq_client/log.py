import logging

from logging.handlers import QueueListener

from multiprocessing import Queue as IPCQueue


TOP_LOGGER_NAME = "rabbitmq_client"

_log_manager = None


def initialize_log_manager(log_level=None):
    """
    Initializes logging for the RabbitMQ client. If no log level is provided,
    warnings and up will be printed to console, no null handler is used.

    :param log_level: log level to use
    """
    global _log_manager
    _log_manager = LogManager(log_level)

    if log_level is not None:
        _log_manager.start()


def get_log_manager():
    """
    Interface for getting the log manager instance of the module.

    :return: log manager or None
    """
    return _log_manager if _log_manager is not None else None


def set_process_log_handler(queue, log_level):
    """
    Sets up the processes "root" logger. Well, at least the root of the
    rabbitmq_client project. Queue will be None in case logging is deactivated.

    The idea is that this function is called from EACH started process, so that
    each process has a rabbitmq_client logger configured and a handler which
    puts all log entires on the process shared queue for the log manager to
    consume.

    :param queue: multiprocessing queue to instantiate the QueueHandler with
    :param log_level: log level that the logger and associated handler should
                      handle
    """
    # Found in test case when running them one after the other. If one test
    # case has enabled logging, the rabbitmq_client logger has an assigned
    # QueueHandler referring to an old IPC Queue.

    # This also works as a way of clearing all handlers when a new process is
    # forked.
    logger = logging.getLogger(TOP_LOGGER_NAME)
    logger.handlers = []

    if queue is not None:
        logger.setLevel(log_level)

        handler = logging.handlers.QueueHandler(queue)
        handler.setLevel(log_level)

        logger.addHandler(handler)


class LogManager:
    """
    This class makes sure that all the processes spawned by the RMQClient can
    use a centralized logging solution. This class' member queue is monitored
    for log messages that a process may want to send to file, which the
    LogHandler then takes care of.
    """

    def __init__(self, log_level, filemode='w'):
        """
        Initializes the log handler by creating a process shared queue.

        :param log_level: sets the log level for the log manager
        :type  log_level: logging.DEBUG | logging.INFO | logging.WARNING |
                          logging.ERROR | logging.CRITICAL
        :param filemode: either write or append. Write will overwrite all
                         previous contents.
        :type  filemode: 'w' | 'a'
        """
        self._log_level = log_level

        if self.log_level is not None:

            self.file_handler = logging.FileHandler(
                "rabbitmq_client.log", mode=filemode
            )
            self.file_handler.setLevel(log_level)
            # Padding log level name to 8 characters, CRITICAL is the longest,
            # centered log level by '^'.
            formatter = logging.Formatter(fmt="{asctime} {levelname:^8} "
                                              "{module} {message}",
                                          style="{",
                                          datefmt="%d/%m/%Y %H:%M:%S")
            self.file_handler.setFormatter(formatter)

            self._log_queue = IPCQueue()

            self._listener = QueueListener(
                self._log_queue, self.file_handler, respect_handler_level=True
            )

    def start(self):
        """
        Starts the log manager by starting the listener on the log queue.
        """
        self._listener.start()

    def stop(self):
        """Stops the log manager."""
        if self.log_level is not None:
            self._listener.stop()

            # To avoid resource warnings, the file handler must be explicitly
            # closed.
            self.file_handler.close()

            # flush and close queue
            while not self._log_queue.empty():
                self._log_queue.get()
            self._log_queue.close()
            # In order for client.stop() to be reliable and consistent, ensure
            # thread stop.
            self._log_queue.join_thread()

    @property
    def log_queue(self):
        """
        Getter for _log_queue.

        :return: the manager's log queue
        """
        if self._log_level is not None:
            return self._log_queue
        else:
            return None


    @property
    def log_level(self):
        """
        Getter for log level.

        :return: the log manager's set log level
        """
        return self._log_level
