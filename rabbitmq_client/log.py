import logging
import logging.handlers


TOP_LOGGER_NAME = "rabbitmq_client"


def set_process_log_handler(log_queue, log_level):
    """
    Sets up the processes "root" logger. Well, at least the root of the
    rabbitmq_client project.

    The idea is that this function is called from EACH started process, so that
    each process has a rabbitmq_client logger configured and a handler which
    puts all log entires on the process shared queue for the an external
    consumer. This is intended to ensure that client users can configure their
    own logging schemes. See README for an example.

    :param log_queue: queue to instantiate the QueueHandler with
    :type log_queue: multiprocessing.Queue
    :param log_level: log level that the logger and associated handler should
                      handle (for example: logging.DEBUG)
    """
    logger = logging.getLogger(TOP_LOGGER_NAME)
    logger.handlers = []  # Clear potential handlers process fork.

    logger.setLevel(log_level)
    handler = logging.handlers.QueueHandler(log_queue)
    handler.setLevel(log_level)
    logger.addHandler(handler)
