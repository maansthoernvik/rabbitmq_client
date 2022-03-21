# RabbitMQ client helpers based on pika
[![PyPI version](https://badge.fury.io/py/rabbitmq-client.svg)](https://badge.fury.io/py/rabbitmq-client)
[![Build](https://github.com/maansthoernvik/rabbitmq_client/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/maansthoernvik/rabbitmq_client/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/maansthoernvik/rabbitmq_client/branch/master/graph/badge.svg?token=R4C6ND9QP2)](https://codecov.io/gh/maansthoernvik/rabbitmq_client)

This project provides helper classes for using RabbitMQ in Python. It is 
based on `pika`, which is an awesome no-dependency client library for 
RabbitMQ. Similarly, this project strives for zero dependencies (except for 
dev dependencies).

By using this project, users should be able to get started with RabbitMQ in 
Python instantly, by simply instantiating and starting a `RMQConsumer` or 
`RMQProducer` class.

## Consumer

``RMQConsumer`` extends the `RMQConnection` base class with only one extra 
method: `consume`. Consume can be passed parameters for declaring queues and
exchanges, as well as binding them together, and consume parameters, all of 
which have corresponding kwargs in the pika library. The idea is not to 
re-invent the wheel, but simply the process of declaring a queue -> declaring an
exchange -> binding the exchange and queue together -> consuming from the queue.

Here is an example:
```python
from rabbitmq_client import RMQConsumer, ConsumeParams, QueueParams


def on_message(msg, ack=None):
    ...

consumer = RMQConsumer()
consumer.start()
consumer.consume(ConsumeParams(on_message),
                 queue_params=QueueParams("queue_name"))
```

The flow of declaring, binding, and consuming is quite 
straightforward. The above example will declare a queue with the name 
"queue_name" and consume from it.

*NOTE!* Although the above may look synchronous, it is not. Start is 
asynchronous 
and any consume started while the consumer is not fully started will simply be
delayed until it is. When a consume has been successfully started, the bound 
callback will receive a ``ConsumeOK`` object containing the resulting 
consumer tag.

### Acknowledging received messages

By default, received messages need to be acknowledged when received. By calling
the ``ack`` kwarg function a message is acknowledged and won't be sent again.
If a message isn't acknowledged using this function, it will be re-sent by
RabbitMQ on consuming from a queue again.

```python
from rabbitmq_client import RMQConsumer, ConsumeParams, QueueParams

from some_other_module import handle_msg


def on_message(msg, ack=None):
    error = handle_msg(msg)
    
    if not error:
        ack()

consumer = RMQConsumer()
consumer.start()
consumer.consume(ConsumeParams(on_message),
                 queue_params=QueueParams("queue_name"))
```

To enable automatic acknowledgement of messages, pass the ``auto_ack`` parameter
to the ``ConsumeParams``, set to ``True``. If ``auto_ack`` is ``True``, the 
``ack`` kwarg is unset.

## Producer
`RMQProducer` extends the `RMQConnection` base class with two additional 
methods: `publish` and `activate_confirm_mode`. Publish is used, as it 
sounds, to publish messages towards queues and/or exchanges. The confirm 
mode activation method enabled confirm mode so that users can verify that 
messages have been delivered successfully.

```python
from rabbitmq_client import RMQProducer, ExchangeParams


def on_confirm(confirmation):
    ...

producer = RMQProducer()
producer.start()
producer.activate_confirm_mode(on_confirm)  # Or don't, depends on your needs

producer.publish(b"body", 
                 exchange_params=ExchangeParams("exchange_name"),
                 routing_key="some.routing.key")
```

`activate_confirm_mode` isn't synchronous either, but you don't have to 
worry about that. Calling `publish` after `activate_confirm_mode` will lead 
to the publish not happening until confirm mode has been activated 
successfully. The callback passed to `activate_confirm_mode` will also 
receive a `ConfirmModeOK` once confirm mode is on. Any publish between 
calling `activate_confirm_mode` and the producer receiving a 
confirm_select_ok from RabbitMQ will be buffered and not issues until 
confirm mode is on. When confirm mode is on, `publish` also returns a key that 
clients can use to correlate successful delivered with calls to publish. 
Once a `publish` call with key X is confirmed, the callback passed to 
`activate_confirm_mode` will be called with X.

## Abstract connection helper

The abstract `RMQConnection` class can be subclassed to get a head start in
using the `pika` `SelectConnection` and `Channel` objects as it wraps them 
and provides an easy-to-use interface as well as event hooks on important
happenings.

### RMQConnection lifecycle hooks

Subclassing `RMQConnection` requires the implementer to override three methods:
`on_ready`, `on_close`, and `on_error`. 

`on_ready` is called when `RMQConnection` has established a connection and 
opened a channel. 

`on_close` is called when either the connection, or the channel closes for
any reason. This means the implementer may receive two calls for one failed 
connection, one for the channel and one for the connection itself. This 
makes it important that `on_close` is made idempotent.   

`on_error` is called when a recent action failed, such as an exchange 
declaration failure. These hooks are meant to enable the implementer to react
to the connection state and restore an operating state. The `RMQConnection` 
abstract base class is used by the `rabbitmq_client` project to implement its
`RMQConsumer` and `RMQProducer` classes.

### RMQConnection interface methods

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

Aside from the connection-related methods, the `RMQConnection` also exposes
interations with the `pika.Channel`, named similarily. See here for what is
exposed: [Pika docs](https://pika.readthedocs.io/en/stable/modules/channel.html).

### Automatic reconnection

`RMQConnection` will re-establish lost connections, but not lost channels. 
Reconnections will not be done for any reason though, among the reasons for 
reconnecting are:

* pika.exceptions.ConnectionClosedByBroker
* pika.exceptions.StreamLostError

These two exceptions cover the cases where the broker has been shut down, either
expectedly or unexpectedly, or when the connection is lost for some other 
reason.

Again, if the channel is lost, but the connection remains intact, 
`RMQConnection` will not recover the channel.

Reconnection attempts will be made with an increasing delay between attempts.
The first attempt is instantaneous, the second is delayed by 1 second, the 
third by 2 seconds, etc. After the 9th attempt, the following reconnects 
will be made at 30 second intervals.

## Logging

`rabbitmq_client` follows Python logging standards and is by default disabled.
To enable logging, attach a handler to `rabbitmq_client`:

```Python
import logging

logging.getLogger("rabbitmq_client").addHandler(logging.StreamHandler())
```

By default, a `logging.NullHandler()` is attached to this logger.
