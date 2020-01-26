# RabbitMQ client based on pika
[![PyPI version](https://badge.fury.io/py/rabbitmq-client.svg)](https://badge.fury.io/py/rabbitmq-client)

This repository offers a RabbitMQ client based on pika. Its purpose is to
provide an extremely simple API with which to interact with RabbitMQ. The idea
is to remove the need for deep knowledge in the workings of RabbitMQ in order to
use it as a messaging service, allowing more people to leverage its power while
avoiding having to invest an inordinate amount of time:

 * Selecting libraries
 * Choosing which interactions (RPC, Pub/Sub, etc.) need to be supported
 * Implementing interactions with client libraries

This RabbitMQ client offers a simple single point of access to RPC and 
Publish/Subscribe functionality.
