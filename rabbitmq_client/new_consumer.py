from rabbitmq_client.new_connection import RMQConnection


class RMQConsumer(RMQConnection):

    def on_ready(self):
        pass

    def on_close(self):
        pass

    def on_error(self):
        pass
