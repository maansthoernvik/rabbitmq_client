from rabbitmq_client.new_connection import RMQConnection


class ConnectionImplementer(RMQConnection):
    def on_ready(self):
        pass

    def on_close(self):
        pass