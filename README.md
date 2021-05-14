# RabbitMQ client based on pika
[![PyPI version](https://badge.fury.io/py/rabbitmq-client.svg)](https://badge.fury.io/py/rabbitmq-client)
[![Build](https://github.com/maansthoernvik/rabbitmq_client/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/maansthoernvik/rabbitmq_client/actions/workflows/build.yml)

This project provides helper classes for using RabbitMQ in Python.

## Abstract connection helper

The abstract `RMQConnection` class can be subclassed to get a head start in
using the `pika` `SelectConnection` and `Channel` objects as it wraps them 
and provides an easy-to-use interface as well as event hooks on important
happenings.

Subclassing `RMQConnection` requires the implementer to override three methods:
`on_ready`, `on_close`, and `on_error`. 

Ready is called when the connection has established a connection and opened a
channel. 

Close is called when either the connection, or the channel closes for
any reason. This means the implementer may receive two calls for one failed 
connection, one for the channel and one for the connection itself. 

Error is called when a recent action failed, such as an exchange declaration
failure. These hooks are meant to enable the implementer to react to the 
connection state and restore an operating state. The `RMQConnection` abstract 
base class is used by the `rabbitmq_client` project to implement its
`RMQConsumer` and `RMQProducer` classes.

In addition to the hooks that need to be implemented by implementing classes,
`RMQConnection` provides three public methods that can be used to interact 
with the connection object: `start`, `restart`, and `stop`.

`start` initiates the connection, establishing a `pika.SelectConnection` and
if that's successful, opening a `pika.Channel` for the opened connection. Once
a channel has been opened, `RMQConnection` will issue a call to `on_ready`. 
Subsequent calls to `start` have no effect if the connection has already been
started.

`restart` closes the open connection and ensures that it is started again once
is has been fully closed. `restart` is only meant to be used on successfully 
established connections, it will have no effect on closed connections. 
`restart` is meant to be used as a means to change `pika.ConnectionParameters`
on the fly.

`stop` permanently closes an open connection and will have no effect on a
closed connection. A connection for which `stop` has been called cannot be
re-used. `on_close` is called once the connection is completely stopped.

TODO: Add information on automatic reconnects.

## Logging

`rabbitmq_client` follows Python logging standards and is by default disabled.
To enable logging, attach a handler to `rabbitmq_client`:

```Python
import logging

logging.getLogger("rabbitmq_client").addHandler(logging.StreamHandler())
```

By default, a `logging.NullHandler()` is attached to this logger.
