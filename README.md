# RabbitMQ client based on pika
[![PyPI version](https://badge.fury.io/py/rabbitmq-client.svg)](https://badge.fury.io/py/rabbitmq-client)

This repository offers a RabbitMQ client based on pika. Its purpose is to provide an extremely simple API with which to interact with RabbitMQ. The idea is to remove the need for deep knowledge of RabbitMQ in order to use it as a messaging service, allowing more people to leverage its power while avoiding having to invest an inordinate amount of time researching before getting started.

## Limitations

For now, this client has been created for a specific purpose, and because of that it really only works well while interacting with other clients of this same repository. For example, the publish/subscribe functionality expects an exchange of type fanout with no additional settings. Should an exchange with the same name as what is being subscribed to already exist, the operation will fail as RabbitMQ spots this difference and throws an error. So, currently this client only works well/at all with other clients of the same type, i.e. other `rabbitmq_client` clients.

It is the intention to generalize the applicability of this client in the future, but for now it is being engineered to support all use cases of these projects:

* https://github.com/megacorpincorporated/hint
* https://github.com/megacorpincorporated/hume

## Supported types of messaging

### Publish/subscribe

Allows for message distribution to whoever is currently listening on a given topic. The publish/subscribe feature does not support confirmed message delivery nor is it intended to, only listening services at the time of publishing will receive the published message.

### RPC

Lets a service define a named RPC queue allowing other services to post messages expecting a reply from the RPC server. The solution leverages RabbitMQs direct reply-to functionality as described here: https://www.rabbitmq.com/direct-reply-to.html. Clients need to provide a reply-to property in order for the RPC server to be able to reply. Implementation is based on the examples provided here: https://www.rabbitmq.com/tutorials/tutorial-six-python.html.

### Command queues

Queues consumed from by a single service, command queues gives a service a way of exposing an API to other services. Commands can be sent to the named queue and the owning service consumes from it.
