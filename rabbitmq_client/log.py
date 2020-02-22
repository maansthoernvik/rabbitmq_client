import logging
from logging.handlers import QueueListener

from multiprocessing import Queue as IPCQueue


TOP_LOGGER_NAME = "rabbitmq_client"

_log_manager = None


def initialize_log_manager(log_level=None):
    """
    Initializes logging for the RabbitMQ client.

    :param log_level: log level to use
    """
    if log_level is not None:
        global _log_manager
        _log_manager = LogManager(log_level)
        _log_manager.start()

        logger = logging.getLogger(TOP_LOGGER_NAME)
        handler = logging.handlers.QueueHandler(_log_manager.get_log_queue())
        handler.setLevel(log_level)
        logger.addHandler(handler)


def set_process_log_handler(queue):
    """
    Sets up the processes "root" logger. Well, at least the root of the
    rabbitmq_client project. Queue will be None in case logging is deactivated.

    :param queue: multiprocessing queue to instantiate the QueueHandler with
    """
    if queue is not None:
        logger = logging.getLogger(TOP_LOGGER_NAME)
        handler = logging.handlers.QueueHandler(queue)
        # Top level handler will take care of filtering out irrelevant levels
        handler.setLevel(logging.DEBUG)

        # Clear all previous handlers, important in case of forking method
        logger.handlers = []
        logger.addHandler(handler)


def get_log_queue():
    """
    Interface for getting the multiprocessing queue for logging messages.

    :return: log queue or None if logging is deactivated
    """
    return _log_manager.get_log_queue() if _log_manager is not None else None


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
        file_handler = logging.FileHandler(
            "rabbitmq_client.log", mode=filemode
        )
        file_handler.setLevel(log_level)
        # Padding log level name to 8 characters, CRITICAL is the longest,
        # centered log level by '^'.
        formatter = logging.Formatter(fmt="{asctime} {levelname:^8}: "
                                          "{message}",
                                      style="{",
                                      datefmt="%d/%m/%Y %H:%M:%S")
        file_handler.setFormatter(formatter)

        self._log_queue = IPCQueue()

        self._listener = QueueListener(
            self._log_queue, file_handler, respect_handler_level=True
        )

    def start(self):
        """
        Starts the log manager by starting the listener on the log queue.
        """
        self._listener.start()

    def get_log_queue(self):
        """
        Getter for _log_queue.

        :return: the handler's log queue
        """
        return self._log_queue
