import logging
import signal
import threading

from rabbitmq_client.new_connection import RMQConnection


logger = logging.getLogger("rabbitmq_client")
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
formatter = logging.Formatter(fmt="{asctime} {levelname:^8} "
                                  "{name} {message}",
                              style="{",
                              datefmt="%d/%m/%Y %H:%M:%S")
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


conn = RMQConnection()
conn.start()

event = threading.Event()


def stop(*args):
    conn.stop()
    event.set()


signal.signal(signal.SIGINT, stop)

event.wait()
print("Exiting test program")
