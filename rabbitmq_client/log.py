import logging

from multiprocessing import Queue as IPCQueue
from threading import Thread


LOGGER_NAME = "rabbitmq_client"


class LogItem:
    """
    Encapsulates information of a log entry.
    """
    level = logging.INFO
    content: str
    app_name: str  # Name of logging instance

    def __init__(self, content, app_name, level=logging.INFO):
        """
        Initializes a LogItem to be handled by a LogHandler.

        :param content: log message string.
        :param app_name: logging application's name.
        :param level: level of logging operation. logging.INFO by default.
        """
        self.level = level
        self.content = content
        self.app_name = app_name


class LogClient:
    """
    Used as a proxy for the log queue which each part of the RMQ Client uses.
    This way, some of the repeated code gets put in one place and reduces the
    space taken by each write.
    """

    _log_queue: IPCQueue
    _app_name: str

    def __init__(self, log_queue, app_name):
        """
        :param log_queue: queue which log entries get put in
        :param app_name: application name to tag every log entry
        """
        self._log_queue = log_queue
        self._app_name = app_name

    def get_log_queue(self):
        return self._log_queue

    def debug(self, message):
        """
        Logs a debug message.

        :param message: message to log.
        """
        self._log_queue.put(
            LogItem(message, self._app_name, level=logging.DEBUG)
        )

    def info(self, message):
        """
        Logs an info message.

        :param message: message to log.
        """
        self._log_queue.put(
            LogItem(message, self._app_name, level=logging.INFO)
        )

    def warning(self, message):
        """
        Logs a warning message.

        :param message: message to log.
        """
        self._log_queue.put(
            LogItem(message, self._app_name, level=logging.WARNING)
        )

    def error(self, message):
        """
        Logs an error message.

        :param message: message to log.
        """
        self._log_queue.put(
            LogItem(message, self._app_name, level=logging.ERROR)
        )

    def critical(self, message):
        """
        Logs a critical message.

        :param message: message to log.
        """
        self._log_queue.put(
            LogItem(message, self._app_name, level=logging.CRITICAL)
        )


class LogHandler:
    """
    This class makes sure that all the processes spawned by the RMQClient can
    use a centralized logging solution. This class' member queue is monitored
    for log messages that a process may want to send to file, which the
    LogHandler then takes care of.
    """

    _log_queue: IPCQueue

    def __init__(self, log_queue: IPCQueue, log_level, filemode='w'):
        """
        Initializes the log handler by creating a process shared queue.

        :param log_queue: the IPCQueue which the RMQ client intends to use to
                          post LogItems to the LogHandler.
        :param log_level: minimum log level which will result in written log
                          contents on file.
        :type  log_level: logging.DEBUG | logging.INFO | logging.WARNING |
                          logging.ERROR | logging.CRITICAL
        :param filemode: either write or append. Write will overwrite all
                         previous contents.
        :type  filemode: 'w' | 'a'
        """
        self._log_queue = log_queue

        self._log_level = log_level

        self.logger = logging.getLogger(LOGGER_NAME)
        self.logger.setLevel(log_level)
        self.logger.propagate = False

        file_handler = logging.FileHandler("rabbitmq_client.log", mode=filemode)
        file_handler.setLevel(log_level)
        # Padding log level name to 8 characters, CRITICAL is the longest,
        # centered log level by '^'.
        formatter = logging.Formatter(fmt='{asctime} - {levelname:^8} - '
                                          '{message}', style='{')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def start(self):
        """
        Starts the log handler by monitoring its work queue.
        """
        thread = Thread(target=self._monitor_log_queue, daemon=True)
        thread.start()

    def _monitor_log_queue(self):
        """
        Shall only be the target of the LogHandler's start method. This function
        starts an infinite loop of monitoring the _log_queue for new items to
        log.
        """
        while True:
            # Blocking, set block=False to not block
            log_item = self._log_queue.get()
            self.handle_log_item(log_item)

    def handle_log_item(self, log_item: LogItem):
        """
        Handles a single log item by dispatching it to the designated logger
        instance.

        :param log_item: log item to handle
        """
        msg = "{}  {}".format(log_item.app_name, log_item.content)
        self.logger.log(log_item.level, msg)

    def get_log_queue(self):
        """
        Getter for _log_queue.

        :return: the handler's log queue
        """
        return self._log_queue
