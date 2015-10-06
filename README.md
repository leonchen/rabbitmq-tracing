# RabbitMQ Tracing
Logging based on RabbitMQ with its firehose feature.

[RabbitMQ Firehose Doc](https://www.rabbitmq.com/firehose.html)
[amqp-tracer Doc](https://github.com/leonchen/node-amqp-tracer)

## Config

See sample configs in config.yaml.example.

Storages can be either or both of the folowing 2 types:

1. Recent

  Recent storage uses redis as its backend, and it supports a ttl configuration (in seconds) to set the time period of the messaging events would be saved. Recent storage saves the timestamp and messageId info only.
  
2. Persist

  Persist storage uses monogdb as its backend. It will save more details of the messaging events than recent storage.
