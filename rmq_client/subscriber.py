from threading import Thread

from kombu import Queue, Exchange
from kombu.mixins import ConsumerMixin


class Subscriber(ConsumerMixin):

    consumers = []

    def __init__(self, connection):
        self.connection = connection

    def get_consumers(self, consumer_class, channel):
        consumer_list = []
        for consumer in self.consumers:
            (topic, routing_key, callback,) = consumer

            consumer_list.append(
                consumer_class(
                    queues=[Queue(name="",
                                  exchange=Exchange(name=topic, type="topic"),
                                  routing_key=routing_key,
                                  exclusive=True)],
                    callbacks=[callback])
            )

        return consumer_list

    def subscribe(self, topic, routing_key, callback):
        self.consumers.append((topic, routing_key, callback,))

        print("Consumer queues: {}".format(self.consumers))

    def start_consuming(self):
        thread = Thread(target=self.run, daemon=True)
        thread.start()
